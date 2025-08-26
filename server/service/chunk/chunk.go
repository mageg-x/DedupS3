package chunk

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	fastcdc "github.com/PlakarKorp/go-cdc-chunkers"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/meta"
)

var (
	instance *ChunkService
	mu       = sync.Mutex{}
)

type ChunkService struct {
	kvstore kv.KVStore
}

func GetChunkService() *ChunkService {
	mu.Lock()
	defer mu.Unlock()
	if instance != nil && instance.kvstore != nil {
		return instance
	}

	store, err := kv.GetKvStore()
	if err != nil || store == nil {
		logger.GetLogger("boulder").Errorf("failed to get kv store: %v", err)
		return nil
	}
	instance = &ChunkService{
		kvstore: store,
	}

	return instance
}

func (s *ChunkService) DoChunk(r io.Reader, bucket, objkey string) error {
	// 进行chunk切分
	// 添加超时控制
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	// 创建输出通道
	chunkChan := make(chan *meta.Chunk, 10)

	// 配置分块器选项
	opts := &fastcdc.ChunkerOpts{
		MinSize:    8 * 1024,
		MaxSize:    128 * 1024,
		NormalSize: 16 * 1024,
	}

	// 启动goroutine处理分块
	go func() {
		defer close(chunkChan)
		// 直接传递 r.Body (io.ReadCloser) 给期望 io.Reader 的函数
		err := s.Split(r, chunkChan, opts)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s Chunking failed: %v", bucket, objkey, err)
		}
		logger.GetLogger("boulder").Tracef("split %s/%schunk finished", bucket, objkey)
	}()

	// 初始化 MD5 哈希用于计算整个数据块的MD5
	fullMD5 := md5.New()
	// 从通道读取并处理块
	processing := true
	for processing {
		select {
		case chunk, ok := <-chunkChan:
			if !ok {
				// 通道已关闭，退出循环
				processing = false
				break
			}
			logger.GetLogger("boulder").Debugf("get cdc chunk %d fp %s", chunk.Size, chunk.Hash)
			// 更新整个数据块的 MD5
			fullMD5.Write(chunk.Data)
			// 处理每个块

		case <-ctx.Done():
			// 超时或取消，退出循环
			logger.GetLogger("boulder").Warnf("Chunking timed out or cancelled: %v", ctx.Err())
			return fmt.Errorf("chunking timed out or cancelled for %s/%s: %v", bucket, objkey, ctx.Err())
		}
	}
	// 计算整个数据块的 MD5
	fullMD5Sum := fullMD5.Sum(nil)
	fullMD5Hex := hex.EncodeToString(fullMD5Sum)
	logger.GetLogger("boulder").Infof("%s/%s full data MD5: %s", bucket, objkey, fullMD5Hex)
	return nil
}

// Split DoChunk 简单的CDC分块函数
func (s *ChunkService) Split(r io.Reader, outputChan chan *meta.Chunk, opt *fastcdc.ChunkerOpts) error {
	// 创建CDC分块器
	chunker, err := fastcdc.NewChunker("fastcdc", r, opt)
	if err != nil {
		return fmt.Errorf("error creating chunker: %v", err)
	}

	// 循环读取并分块
	for {
		chunkData, err := chunker.Next()
		if err != nil && err != io.EOF {
			return fmt.Errorf("cdc chunk error: %v", err)
		}

		// 如果有数据，发送到输出通道
		if len(chunkData) > 0 {
			outputChan <- meta.NewChunk(chunkData)
		}

		// 检查是否结束
		if err == io.EOF {
			break
		}
	}

	return nil
}
