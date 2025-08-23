// Package block /*
package block

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	xconf "github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
	"io"
	"path"
	"time"
)

// S3Store 实现基于S3的存储后端
type S3Store struct {
	client *s3.Client
	conf   *xconf.S3Config
	ctx    context.Context
}

// NewS3Store NewS3Storage 创建新的S3存储后端
func NewS3Store(c *xconf.S3Config) (*S3Store, error) {
	logger.GetLogger("boulder").Infof("Creating new S3 store with bucket: %s", c.Bucket)

	if c == nil {
		logger.GetLogger("boulder").Errorf("NewS3Store: nil config")
		return nil, fmt.Errorf("NewDiskStore: nil config")
	}
	ctx := context.Background()

	if c.AccessKey == "" || c.SecretKey == "" {
		logger.GetLogger("boulder").Errorf("Missing AWS credentials")
		return nil, fmt.Errorf("missing AWS credentials")
	}

	// 创建凭证
	credentialProvider := credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")

	// 加载配置，并指定凭证提供者
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(c.Region),
		config.WithCredentialsProvider(credentialProvider),
	)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to load SDK configuration: %v", err)
		return nil, fmt.Errorf("failed to load SDK configuration: %v", err)
	}

	// 创建 S3 客户端（对接 MinIO 等私有服务需加选项）
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// 如果配置了自定义端点，设置自定义端点
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
			logger.GetLogger("boulder").Debugf("Using custom endpoint: %s", c.Endpoint)
		}
		// 对接 MinIO/OSS/COS 等需要PathStyle的情况
		o.UsePathStyle = c.UsePathStyle
		logger.GetLogger("boulder").Debugf("Use path style: %v", c.UsePathStyle)
	})

	logger.GetLogger("boulder").Infof("S3 store initialized successfully")
	return &S3Store{
		client: client,
		conf:   c,
		ctx:    ctx,
	}, nil
}

// Type 返回存储类型
func (s *S3Store) Type() string {
	return "s3"
}

// WriteBlock 写入块到S3
func (s *S3Store) WriteBlock(blockID string, data []byte) error {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	key := s.blockKey(blockID)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.conf.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})

	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to write block %s to S3: %v", blockID, err)
		return fmt.Errorf("failed to write block %s to S3: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully wrote block to S3: %s", blockID)
	return nil
}

// ReadBlock 从S3读取块
func (s *S3Store) ReadBlock(blockID string, offset, length int64) ([]byte, error) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	key := s.blockKey(blockID)

	// 处理范围请求
	rangeHeader := ""
	if length > 0 {
		rangeHeader = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
		logger.GetLogger("boulder").Debugf("Using range header: %s", rangeHeader)
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(s.conf.Bucket),
		Key:    aws.String(key),
	}

	if rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}

	resp, err := s.client.GetObject(ctx, input)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to read block %s from S3: %v", blockID, err)
		return nil, fmt.Errorf("failed to read block %s from S3: %w", blockID, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to read response body for block %s: %v", blockID, err)
		return nil, fmt.Errorf("failed to read response body for block %s: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully read block from S3: %s, read %d bytes", blockID, len(data))
	return data, nil
}

// DeleteBlock 删除S3块
func (s *S3Store) DeleteBlock(blockID string) error {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	key := s.blockKey(blockID)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.conf.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete block %s from S3: %v", blockID, err)
		return fmt.Errorf("failed to delete block %s from S3: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully deleted block from S3: %s", blockID)
	return nil
}

// BlockExists 检查块是否存在
func (s *S3Store) BlockExists(blockID string) (bool, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	key := s.blockKey(blockID)
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.conf.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			logger.GetLogger("boulder").Debugf("Block %s does not exist in S3", blockID)
			return false, nil
		}
		logger.GetLogger("boulder").Errorf("Failed to check if block %s exists: %v", blockID, err)
		return false, fmt.Errorf("failed to check if block %s exists: %w", blockID, err)
	}
	logger.GetLogger("boulder").Debugf("Block %s exists in S3", blockID)
	return true, nil
}

// HealthCheck 检查S3连接是否正常
func (s *S3Store) HealthCheck() error {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.conf.Bucket),
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("S3 health check failed: %v", err)
		return fmt.Errorf("S3 health check failed: %w", err)
	}
	logger.GetLogger("boulder").Debugf("S3 health check passed")
	return nil
}

// Location 获取块位置
func (s *S3Store) Location(blockID string) string {
	return fmt.Sprintf("s3://%s/%s", s.conf.Bucket, s.blockKey(blockID))
}

// blockKey 获取块在S3中的键
func (s *S3Store) blockKey(blockID string) string {
	// 使用两级目录分散对象
	if len(blockID) < 4 {
		// 处理短 blockID 的情况
		logger.GetLogger("boulder").Debugf("Short blockID detected: %s", blockID)
		return path.Join("blocks", blockID)
	}
	dir1 := blockID[:2]
	dir2 := blockID[2:4]
	return path.Join("blocks", dir1, dir2, blockID)
}
