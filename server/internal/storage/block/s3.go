// Package block /*
package block

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	xconf "github.com/mageg-x/boulder/internal/config"
	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
)

// S3Store 实现基于S3的存储后端
type S3Store struct {
	BaseBlockStore
	client   *s3.Client
	uploader *manager.Uploader
	conf     *xconf.S3Config
	ctx      context.Context
}

// NewS3Store NewS3Storage 创建新的S3存储后端
func NewS3Store(c *xconf.S3Config) (*S3Store, error) {
	logger.GetLogger("boulder").Infof("Creating new S3 store with bucket: %#v", c)

	ctx := context.Background()

	if c.AccessKey == "" || c.SecretKey == "" || c.Endpoint == "" {
		logger.GetLogger("boulder").Errorf("Missing AWS credentials")
		return nil, fmt.Errorf("missing AWS credentials")
	}

	// 创建凭证
	credentialProvider := credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")

	httpcli := &http.Client{
		Transport: &xhttp.HttpLoggingTransport{
			Transport: http.DefaultTransport,
		},
	}
	// 加载配置，并指定凭证提供者
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(c.Region),
		config.WithCredentialsProvider(credentialProvider),
		config.WithHTTPClient(httpcli),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		// 关闭所有 SDK 日志
		config.WithLogger(logger.AWSNullLogger{}),
	)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to load SDK configuration: %v", err)
		return nil, fmt.Errorf("failed to load SDK configuration: %w", err)
	}

	// 创建 S3 客户端（对接 MinIO 等私有服务需加选项）
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(c.Endpoint)
		o.UsePathStyle = c.UsePathStyle
		o.RetryMaxAttempts = 1 // 禁用重试
		logger.GetLogger("boulder").Debugf("Use path style: %v", c.UsePathStyle)
	})

	// 创建 Uploader
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.Concurrency = 12           // 改为 12 路并发
		u.PartSize = 5 * 1024 * 1024 // 5MB 每片
	})
	uploader.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired

	// 创建S3Store实例
	s3 := &S3Store{
		client:   client,
		uploader: uploader,
		conf:     c,
		ctx:      ctx,
	}

	logger.GetLogger("boulder").Infof("S3 store initialized successfully")
	return s3, nil
}

// Type 返回存储类型
func (s *S3Store) Type() string {
	return "s3"
}

// WriteBlock 写入块到S3
func (s *S3Store) WriteBlock(ctx context.Context, blockID string, data []byte, ver int32) error {
	logger.GetLogger("boulder").Debugf("[S3Store WriteBlock] blockID=%s, ver=%d, size=%d KB", blockID, ver, len(data)/1024)

	vfile, err := GetTieredFs()
	if err != nil || vfile == nil {
		logger.GetLogger("boulder").Errorf("failed to get tiered fs: %v", err)
		return fmt.Errorf("failed to get tiered fs: %v", err)
	}

	oldVer := int32(-1)
	if vfile.Exists(blockID) {
		if v, err := vfile.ReadFile(blockID, 0, 4); err == nil && v != nil {
			oldVer = int32(binary.BigEndian.Uint32(v[:]))
			logger.GetLogger("boulder").Debugf("get block %s old ver %d", blockID, oldVer)
		}
	}

	if ver <= oldVer {
		return nil
	}

	// 创建新数据：4字节版本号 + 序列化数据
	versionBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(versionBuf, uint32(ver))

	if err := vfile.WriteFile(blockID, [][]byte{versionBuf, data}, ver); err != nil {
		logger.GetLogger("boulder").Errorf("failed to write block %s: %v", blockID, err)
		return fmt.Errorf("failed to write block %s: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully wrote block: %s", blockID)
	return nil
}

func (s *S3Store) WriteBlockDirect(ctx context.Context, blockID string, data []byte) error {
	key := s.BlockPath(blockID)

	if len(data) <= 4 {
		return fmt.Errorf("invalid block size: %d", len(data))
	}

	verBuf := data[:4]
	data = data[4:]
	ver := int32(binary.BigEndian.Uint32(verBuf[:]))

	//_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
	//	Bucket: aws.String(s.conf.Bucket),
	//	Key:    aws.String(key),
	//	Body:   bytes.NewReader(data),
	//})

	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.conf.Bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))), // 明确指定长度，禁用 aws-chunked
		// 可以添加额外的元数据标识版本
		Metadata: map[string]string{
			"block-version": fmt.Sprintf("%d", ver),
			"block-id":      blockID,
		},
	})

	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to write block %s  %s  %s len :%d to S3 failed error is %v, config is %#v", s.conf.Bucket, key, blockID, len(data), err, s.conf)
		return fmt.Errorf("failed to write block %s to S3: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully wrote block to S3: %s", blockID)
	return nil
}

func (s *S3Store) ReadBlock(location, blockID string, offset, length int64) ([]byte, error) {
	data, err := s.ReadS3Block(blockID, offset, length)
	if err != nil {
		//if errors.Is(err, ErrBlockNotFound)
		{
			// S3 上没有， 还在节点缓存中未提交
			data, err = s.ReadRemoteBlock(location, blockID, offset, length)
			if err != nil {
				// 再从S3 试一次
				data, err = s.ReadS3Block(blockID, offset, length)
				if err != nil {
					logger.GetLogger("boulder").Errorf("read block %s failed: %v", blockID, err)
				}
			}
		}
	}

	return data, err
}

// ReadBlock 从S3读取块
func (s *S3Store) ReadS3Block(blockID string, offset, length int64) ([]byte, error) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	key := s.BlockPath(blockID)

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
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound" {
				// 确定是对象不存在的错误
				logger.GetLogger("boulder").Debugf("Block %s does not exist in S3", blockID)
				return nil, ErrBlockNotFound
			}
		}
		logger.GetLogger("boulder").Infof("Failed to read block %s from S3: %v", blockID, err)
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

	key := s.BlockPath(blockID)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.conf.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NotFound" {
				return nil
			}
		}

		return fmt.Errorf("failed to delete block %s from S3: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully deleted block from S3: %s", blockID)
	return nil
}

// BlockExists 检查块是否存在
func (s *S3Store) BlockExists(blockID string) (bool, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	key := s.BlockPath(blockID)
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
		logger.GetLogger("boulder").Errorf("s3 health check failed: %v", err)
		return fmt.Errorf("s3 health check failed: %w", err)
	}

	logger.GetLogger("boulder").Debugf("S3 health check passed")
	return nil
}

// Location 获取块位置
func (s *S3Store) Location(blockID string) string {
	return fmt.Sprintf("s3://%s/%s", s.conf.Bucket, s.BlockPath(blockID))
}

// List 使用分页方式列出S3存储中的所有块，流式返回 blockID
func (s *S3Store) List() (<-chan string, <-chan error) {
	blockChan := make(chan string, 100) // 增加缓冲，避免阻塞
	errChan := make(chan error, 1)

	go func() {
		defer close(blockChan)
		defer close(errChan)

		const batchSize = 1000
		var continuationToken *string
		isTruncated := true
		blockPrefix := "blocks/"

		logger.GetLogger("boulder").Infof("Starting to list blocks in S3 store: bucket=%s, prefix=%s", s.conf.Bucket, blockPrefix)

		totalBlocks := 0
		pageCount := 0

		for isTruncated {
			pageCount++
			logger.GetLogger("boulder").Debugf("Listing S3 page %d with continuation token: %v", pageCount, continuationToken)

			resp, err := s.client.ListObjectsV2(s.ctx, &s3.ListObjectsV2Input{
				Bucket:            aws.String(s.conf.Bucket),
				Prefix:            aws.String(blockPrefix),
				MaxKeys:           aws.Int32(batchSize),
				ContinuationToken: continuationToken,
			})

			if err != nil {
				logger.GetLogger("boulder").Errorf("Error listing S3 objects: %v", err)
				errChan <- fmt.Errorf("error listing S3 objects: %w", err)
				return
			}

			// 优化：直接从 Key 末尾提取 blockID，避免 Split 分配
			pageBlocks := 0
			for _, obj := range resp.Contents {
				key := aws.ToString(obj.Key)

				// 路径格式：blocks/xx/yy/zz/blockID → blockID 是最后一段
				lastSlash := strings.LastIndex(key, "/")
				if lastSlash == -1 || lastSlash >= len(key)-1 {
					continue // 无效路径
				}
				blockID := key[lastSlash+1:]

				// 只检查长度，不依赖目录层级
				if len(blockID) < 20 {
					logger.GetLogger("boulder").Debugf("Skipping invalid block ID (too short): %s", blockID)
					continue
				}

				pageBlocks++
				totalBlocks++

				select {
				case blockChan <- blockID:
				case <-s.ctx.Done():
					logger.GetLogger("boulder").Debugf("Context canceled while listing, total sent: %d", totalBlocks)
					return
				}
			}

			logger.GetLogger("boulder").Debugf("Processed page %d, found %d blocks, total: %d", pageCount, pageBlocks, totalBlocks)

			// 更新分页状态
			continuationToken = resp.ContinuationToken
			isTruncated = aws.ToBool(resp.IsTruncated) // 显式转换

			// 每 10 页休息一下，避免请求过密
			if pageCount%10 == 0 {
				time.Sleep(100 * time.Millisecond)
			}
		}

		logger.GetLogger("boulder").Infof("Finished listing blocks, total: %d", totalBlocks)
	}()

	return blockChan, errChan
}

// blockKey 获取块在S3中的键
func (s *S3Store) BlockPath(blockID string) string {
	n := len(blockID)
	dir1 := blockID[n-3:]      // 最后3位
	dir2 := blockID[n-6 : n-3] // 倒数第4-6位
	dir3 := blockID[n-9 : n-6] // 倒数第7-9位
	return path.Join("dedups3", "blocks", dir1, dir2, dir3, blockID)
}
