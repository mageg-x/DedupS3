// Package block /*
package block

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/mageg-x/boulder/internal/utils"
)

// S3Store 实现基于S3的存储后端
type S3Store struct {
	BaseBlockStore
	client          *s3.Client
	uploader        *manager.Uploader
	conf            *xconf.S3Config
	ctx             context.Context
	pendingWrites   sync.Map   // 用于跟踪延迟写入
	pwLocker        sync.Mutex // 保护pendingWrites的操作
	monitorStopChan chan struct{}
	monitorRunning  atomic.Bool
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
		logger.GetLogger("boulder").Debugf("Use path style: %v", c.UsePathStyle)
	})

	// 创建 Uploader
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.Concurrency = 12           // 改为 12 路并发
		u.PartSize = 5 * 1024 * 1024 // 5MB 每片
	})
	uploader.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired

	// 创建S3Store实例
	s := &S3Store{
		client:          client,
		uploader:        uploader,
		conf:            c,
		ctx:             ctx,
		pendingWrites:   sync.Map{},
		pwLocker:        sync.Mutex{},
		monitorStopChan: make(chan struct{}),
		monitorRunning:  atomic.Bool{},
	}

	// 启动监控，每秒检查一次
	s.StartMonitor(1 * time.Second)

	logger.GetLogger("boulder").Infof("S3 store initialized successfully")
	return s, nil
}

// Type 返回存储类型
func (s *S3Store) Type() string {
	return "s3"
}

// StartMonitor 添加监控方法，定期检查pendingWrites中的僵尸任务
func (s *S3Store) StartMonitor(interval time.Duration) {
	logger.GetLogger("boulder").Infof("S3Store monitor started with interval %v", interval)
	if s.monitorRunning.Swap(true) {
		return // 已经在运行
	}

	s.monitorStopChan = make(chan struct{})

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer s.monitorRunning.Store(false)

		for {
			select {
			case <-ticker.C:
				// 使用 break 跳出 select，保持代码清晰
				break
			case <-s.monitorStopChan:
				return
			}

			var total int
			var zombies []*pendingWrite
			currentTime := time.Now().UTC()
			s.pendingWrites.Range(func(key, value interface{}) bool {
				total++
				pw := value.(*pendingWrite)
				// 检测僵尸任务：运行时间超过30秒的非活跃任务
				if currentTime.Sub(pw.startTime) > 30*time.Second {
					// 检查任务是否真的僵死了（context是否已取消）
					select {
					case <-pw.ctx.Done():
						// context已取消，确实是僵尸
						zombies = append(zombies, pw)
					default:
						// 只有writing 状态才耗时
						if pw.status != "writing" {
							zombies = append(zombies, pw) // 疑似僵尸
						} else {
							logger.GetLogger("boulder").Errorf("long time pengding writing routine for block %s", pw.blockID)
						}
					}
				}
				return true
			})

			// 打印监控信息
			logger.GetLogger("boulder").Errorf("[S3Store pendingWrite] Total:%d, Zombies:%d", total, len(zombies))
			for _, _pw := range zombies {
				if _pw.cancel != nil {
					_pw.cancel()
				}
				s.pendingWrites.CompareAndDelete(_pw.blockID, _pw)
			}
		}
	}()
}

// StopMonitor 停止监控
func (s *S3Store) StopMonitor() {
	if s.monitorRunning.Load() {
		close(s.monitorStopChan)
	}
}

// WriteBlock 写入块到S3
func (s *S3Store) WriteBlock(ctx context.Context, blockID string, data []byte, ver int32) error {
	return s.WriteBlockDelay(ctx, blockID, data, ver)
}

func (s *S3Store) WriteBlockDelay(ctx context.Context, blockID string, data []byte, ver int32) error {
	logger.GetLogger("boulder").Debugf("[S3 WriteBlockDelay] blockID=%s, ver=%d, size=%d KB", blockID, ver, len(data)/1024)

	var newPw *pendingWrite
	err := utils.WithLock(&s.pwLocker, func() error {
		// 尝试加载已存在的 pending 任务
		if old, loaded := s.pendingWrites.Load(blockID); loaded {
			pw := old.(*pendingWrite)
			// 如果新版本 >= 旧版本，取消旧写入
			if ver >= pw.ver {
				// 更新状态为取消
				pw.status = "canceled"
				pw.cancel() // 取消旧的写入 goroutine
			} else {
				// 新版本更老，直接丢弃（防止降级）
				logger.GetLogger("boulder").Debugf("[S3 WriteBlockDelay] discard older version, blockID=%s, newVer=%d, oldVer=%d", blockID, ver, pw.ver)
				return fmt.Errorf("older version discarded") // 明确返回错误
			}
		}

		// 创建新的可取消 context 用于控制延迟写入
		writeCtx, cancel := context.WithCancel(ctx)
		// 创建新的pendingWrite实例
		newPw = &pendingWrite{
			ctx:       writeCtx,
			cancel:    cancel,
			ver:       ver,
			startTime: time.Now().UTC(),
			blockID:   blockID,
			status:    "pending",
		}
		// 保存到map中
		s.pendingWrites.Store(blockID, newPw)
		return nil
	})

	if err != nil || newPw == nil {
		return err
	}

	// 启动延迟写入routine，合并覆写情况
	go func(_pw *pendingWrite, _data []byte) {
		defer func() {
			// 更新状态并清理
			if r := recover(); r != nil {
				_pw.status = "panic"
				logger.GetLogger("boulder").Errorf("[S3 WriteBlockDelay] panic in goroutine, blockID=%s, error=%v", _pw.blockID, r)
			}
			if _pw.cancel != nil {
				_pw.cancel()
			}
			// 安全删除：只有当当前记录仍然是我们的pendingWrite时才删除
			s.pendingWrites.CompareAndDelete(_pw.blockID, _pw)
		}()
		// 更新状态为等待延迟
		_pw.status = "waiting_delay"

		// 非终结块，需延迟写
		if _pw.ver != 0x07FFFF {
			delayTime := 5000 * time.Millisecond
			// 等待 delayTime
			select {
			case <-time.After(delayTime):
				// 延迟结束，继续
				_pw.status = "delay_completed"
			case <-_pw.ctx.Done():
				_pw.status = "context_canceled"
				return // 被取消
			}
		}

		utils.WithLockKey(_pw.blockID, func() error {
			// 锁内立即二次检查 cancel
			select {
			case <-_pw.ctx.Done():
				_pw.status = "context_canceled"
				return nil
			default:
			}

			// 获取锁后，并二次检查
			if old, loaded := s.pendingWrites.Load(_pw.blockID); loaded {
				curPw := old.(*pendingWrite)
				if curPw.ver > _pw.ver {
					_pw.status = "overridden_newer"
					return nil // 已被更高版本覆盖
				}
			} else {
				_pw.status = "already_processed"
				// 已经被其他goroutine处理了
				return nil
			}

			// 此时才安全执行写入
			_pw.status = "writing"
			if err := s.WriteBlockDirect(_pw.ctx, _pw.blockID, _data, _pw.ver); err != nil {
				_pw.status = "write_failed"
				logger.GetLogger("boulder").Errorf("[S3 WriteBlockDelay] upload failed, blockID=%s, ver=%d, error=%v", _pw.blockID, _pw.ver, err)
				return err
			}
			_pw.status = "write_completed"
			logger.GetLogger("boulder").Debugf("[S3 WriteBlockDelay] upload completed, blockID=%s, ver=%d", _pw.blockID, _pw.ver)
			return nil
		})
	}(newPw, data)

	return nil
}

func (s *S3Store) WriteBlockDirect(ctx context.Context, blockID string, data []byte, ver int32) error {
	key := s.blockKey(blockID)

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
		if errors.Is(err, ErrBlockNotFound) {
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
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound" {
				// 确定是对象不存在的错误
				logger.GetLogger("boulder").Debugf("Block %s does not exist in S3", blockID)
				return nil, ErrBlockNotFound
			}
		}
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
		logger.GetLogger("boulder").Errorf("s3 health check failed: %v", err)
		return fmt.Errorf("s3 health check failed: %w", err)
	}

	logger.GetLogger("boulder").Debugf("S3 health check passed")
	return nil
}

// Location 获取块位置
func (s *S3Store) Location(blockID string) string {
	return fmt.Sprintf("s3://%s/%s", s.conf.Bucket, s.blockKey(blockID))
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
func (s *S3Store) blockKey(blockID string) string {
	n := len(blockID)
	dir1 := blockID[n-3:]      // 最后3位
	dir2 := blockID[n-6 : n-3] // 倒数第4-6位
	dir3 := blockID[n-9 : n-6] // 倒数第7-9位
	return path.Join("dedups3", "blocks", dir1, dir2, dir3, blockID)
}
