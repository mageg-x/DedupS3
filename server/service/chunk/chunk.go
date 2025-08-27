package chunk

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"github.com/mageg-x/boulder/service/block"
	"io"
	"strings"
	"sync"

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

func (c *ChunkService) DoChunk(r io.Reader, obj *meta.Object) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 创建输出通道
	chunkChan := make(chan *meta.Chunk, 100)

	// 配置分块器选项
	opts := &fastcdc.ChunkerOpts{
		MinSize:    8 * 1024,
		MaxSize:    128 * 1024,
		NormalSize: 16 * 1024,
	}

	// 切分
	go func() {
		defer close(chunkChan)
		// 直接传递 r.Body (io.ReadCloser) 给期望 io.Reader 的函数
		err := c.Split(ctx, r, chunkChan, opts, obj.Bucket, obj.Key)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s split chunk failed: %v", obj.Bucket, obj.Key, err)
			cancel()
			return
		}
		logger.GetLogger("boulder").Tracef("split %s/%schunk finished", obj.Bucket, obj.Key)
	}()

	// 去重
	allChan := make(chan []*meta.Chunk, 1)
	dedupChan := make(chan *meta.Chunk, 100)

	go func() {
		defer close(dedupChan)
		defer close(allChan)
		chunks, err := c.Dedup(ctx, chunkChan, dedupChan, obj.Bucket, obj.Key)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s merge chunk failed: %v", obj.Bucket, obj.Key, err)
			cancel()
			return
		}
		allChan <- chunks
	}()

	rollback := false
	//重组
	blockChunks, err := c.Assemble(ctx, dedupChan, obj.DataLocation, obj.Bucket, obj.Key)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s assemble chunk failed: %v", obj.Bucket, obj.Key, err)
		rollback = true
		cancel()
	}

	// 汇总信息
	allChunk, err := c.Summary(ctx, allChan, blockChunks, obj.Bucket, obj.Key)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s summary chunk failed: %v", obj.Bucket, obj.Key, err)
		rollback = true
		cancel()
	} else {
		logger.GetLogger("boulder").Debugf("%s/%s summary chunk finished %+v", obj.Bucket, obj.Key, allChunk)
	}
	if !rollback {
		err = c.WriteMeta(ctx, obj.Owner.ID, allChunk, blockChunks, obj)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s write meta info failed: %v", obj.Bucket, obj.Key, err)
			rollback = true
			cancel()
		}
	}

	// 回滚处理，删除之前写入的block
	if rollback {
		logger.GetLogger("boulder").Warnf("rollback for %s/%s failed", obj.Bucket, obj.Key)
		gcKey := "aws:gc:blocks:" + strings.ReplaceAll(uuid.New().String(), "-", "")
		var blocks []*meta.Block
		for _, _block := range blockChunks {
			blocks = append(blocks, _block)
		}
		err := c.kvstore.Set(gcKey, &blocks)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s save blocks failed: %v", obj.Bucket, obj.Key, err)
		}
		return fmt.Errorf("dochunk %s/%s failed", obj.Bucket, obj.Key)
	}
	return nil
}

// Split DoChunk 简单的CDC分块函数
func (c *ChunkService) Split(ctx context.Context, r io.Reader, outputChan chan *meta.Chunk, opt *fastcdc.ChunkerOpts, bucket, objkey string) error {
	// 创建CDC分块器
	chunker, err := fastcdc.NewChunker("fastcdc", r, opt)
	if err != nil {
		return fmt.Errorf("error creating chunker: %v", err)
	}

	// 循环读取并分块
	for {
		// 检查 context 是否已取消
		select {
		case <-ctx.Done():
			return fmt.Errorf("fast cdc %s/%s be canceled: %v", bucket, objkey, ctx.Err())
		default:
			// 继续执行
		}

		chunkData, err := chunker.Next()
		if err != nil && err != io.EOF {
			return fmt.Errorf("cdc chunk %s/%s error: %v", bucket, objkey, err)
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

func (c *ChunkService) Dedup(ctx context.Context, inputChan, dedupChan chan *meta.Chunk, bucket, objkey string) ([]*meta.Chunk, error) {
	allChan := make([]*meta.Chunk, 0)
	chunkFilter := make(map[string]bool)
	// 初始化 MD5 哈希用于计算整个数据块的MD5
	fullMD5 := md5.New()
	// 从通道读取并处理块
	finished := false
	for !finished {
		select {
		case <-ctx.Done():
			// 超时或取消，退出循环
			logger.GetLogger("boulder").Warnf("Chunking timed out or cancelled: %v", ctx.Err())
			return nil, fmt.Errorf("chunking timed out or cancelled for %s/%s: %v", bucket, objkey, ctx.Err())
		case chunk, ok := <-inputChan:
			if !ok {
				// 通道已关闭，退出循环
				finished = true
				break
			}
			logger.GetLogger("boulder").Debugf("get cdc chunk %d fp %s", chunk.Size, chunk.Hash)
			// 更新整个数据块的 MD5
			fullMD5.Write(chunk.Data)

			// 对象间去重
			chunkKey := "aws:chunk:" + chunk.Hash
			var _chunk *meta.Chunk
			exists, err := c.kvstore.Get(chunkKey, &_chunk)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to get chunk %s: %v", chunk.Hash, err)
				return nil, fmt.Errorf("failed to get chunk %s: %v", chunk.Hash, err)
			}
			if exists {
				chunkFilter[chunk.Hash] = true
				allChan = append(allChan, &meta.Chunk{Hash: chunk.Hash, BlockID: _chunk.BlockID})
				continue
			}

			allChan = append(allChan, &meta.Chunk{Hash: chunk.Hash, BlockID: ""})
			// 对象内去重
			if chunkFilter[chunk.Hash] {
				continue
			}

			chunkFilter[chunk.Hash] = true
			dedupChan <- chunk
		}
	}
	// 计算整个数据块的 MD5
	fullMD5Sum := fullMD5.Sum(nil)
	fullMD5Hex := hex.EncodeToString(fullMD5Sum)
	logger.GetLogger("boulder").Infof("%s/%s full data MD5: %s", bucket, objkey, fullMD5Hex)
	return allChan, nil
}

func (c *ChunkService) Assemble(ctx context.Context, dedupChan chan *meta.Chunk, storageID, bucket, objkey string) (map[string]*meta.Block, error) {
	blockChunks := make(map[string]*meta.Block)
	finished := false
	for !finished {
		select {
		case <-ctx.Done():
			logger.GetLogger("boulder").Warnf(" assemble chunk be cancelled: %v", ctx.Err())
			return blockChunks, fmt.Errorf("assemble chunk be cancelled for %s/%s: %v", bucket, objkey, ctx.Err())
		case chunk, ok := <-dedupChan:
			bs := block.GetBlockService()
			if bs == nil {
				logger.GetLogger("boulder").Errorf("%s/%s block service is nil", bucket, objkey)
				return blockChunks, fmt.Errorf("%s/%s block service is nil", bucket, objkey)
			}

			_block, err := bs.PutChunk(chunk, storageID, bucket, objkey)
			if err != nil {
				logger.GetLogger("boulder").Errorf("%s/%s put chunk failed: %v", bucket, objkey, err)
				return blockChunks, fmt.Errorf("%s/%s put chunk failed: %v", bucket, objkey, err)
			}

			if _block != nil {
				blockChunks[_block.ID] = _block
			}

			if !ok || chunk == nil {
				finished = true
				break
			}
		}
	}
	return blockChunks, nil
}

func (c *ChunkService) Summary(ctx context.Context, allChan chan []*meta.Chunk, blockChunks map[string]*meta.Block, bucket, objkey string) ([]*meta.Chunk, error) {
	allChunks := make([]*meta.Chunk, 0)
	chunk2block := make(map[string]string)
	for _, _block := range blockChunks {
		for _, _chunk := range _block.ChunkList {
			chunk2block[_chunk.Hash] = _block.ID
		}
	}

	select {
	case <-ctx.Done():
		logger.GetLogger("boulder").Errorf("%s/%s do chunk be canceled", bucket, objkey)
		return allChunks, fmt.Errorf("%s/%s do chunk be canceled", bucket, objkey)
	case result := <-allChan:
		if result == nil {
			logger.GetLogger("boulder").Errorf("%s/%s dedup chunk failed", bucket, objkey)
			return allChunks, fmt.Errorf("%s/%s dedup chunk failed", bucket, objkey)
		}
		for _, chunk := range result {
			if chunk == nil {
				logger.GetLogger("boulder").Errorf("%s/%s dedup chunk failed", bucket, objkey)
				return allChunks, fmt.Errorf("%s/%s dedup chunk failed", bucket, objkey)
			}

			if chunk2block[chunk.Hash] != "" {
				chunk.BlockID = chunk2block[chunk.Hash]
			}

			if chunk.BlockID == "" {
				logger.GetLogger("boulder").Errorf("%s/%s dedup chunk failed", bucket, objkey)
				return allChunks, fmt.Errorf("%s/%s dedup chunk failed", bucket, objkey)
			}
			allChunks = append(allChunks, chunk)
		}
		break
	}
	return allChunks, nil
}

func (c *ChunkService) WriteMeta(ctx context.Context, accountID string, allChunk []*meta.Chunk, blockChunks map[string]*meta.Block, obj *meta.Object) error {
	txn, err := c.kvstore.BeginTxn(ctx, nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s create transaction failed: %v", obj.Bucket, obj.Key, err)
		return fmt.Errorf("%s/%s create transaction failed: %v", obj.Bucket, obj.Key, err)
	}
	defer txn.Rollback()

	// 写入chunk元数据
	for _, chunk := range allChunk {
		chunkey := "aws:chunk:" + chunk.Hash
		var _chunk meta.Chunk
		exists, e := txn.Get(chunkey, &_chunk)
		if e != nil {
			logger.GetLogger("boulder").Errorf("%s/%s get chunk failed: %v", obj.Bucket, obj.Key, err)
			return fmt.Errorf("%s/%s get chunk failed: %v", obj.Bucket, obj.Key, err)
		}
		if exists {
			_chunk.RefCount += 1
			e := txn.Set(chunkey, &_chunk)
			if e != nil {
				logger.GetLogger("boulder").Errorf("%s/%s set chunk failed: %v", obj.Bucket, obj.Key, err)
				return fmt.Errorf("%s/%s set chunk failed: %v", obj.Bucket, obj.Key, err)
			} else {
				logger.GetLogger("boulder").Debugf("%s/%s refresh set chunk: %v", obj.Bucket, obj.Key, chunk)
			}
		} else {
			chunk.RefCount = 1
			e := txn.Set(chunkey, &_chunk)
			if e != nil {
				logger.GetLogger("boulder").Errorf("%s/%s set chunk failed: %v", obj.Bucket, obj.Key, err)
				return fmt.Errorf("%s/%s set chunk failed: %v", obj.Bucket, obj.Key, err)
			} else {
				logger.GetLogger("boulder").Debugf("%s/%s add  chunk: %v", obj.Bucket, obj.Key, chunk)
			}
		}
	}

	// 写入桶的元数据
	for _, item := range blockChunks {
		blockKey := "aws:block:" + item.ID
		err = txn.Set(blockKey, &item)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s/%s set block meta failed: %v", obj.Bucket, obj.Key, item.ID, err)
			return fmt.Errorf("%s/%s set block meta failed: %v", obj.Bucket, obj.Key, err)
		} else {
			logger.GetLogger("boulder").Debugf("%s/%s/%s set block meta   ok", obj.Bucket, obj.Key, item.ID)
		}
	}

	//写入object meta信息
	objKey := "aws:object:" + accountID + ":" + obj.Bucket + "/" + obj.Key
	for _, _chunk := range allChunk {
		obj.Chunks = append(obj.Chunks, meta.ObjChunk{Hash: _chunk.Hash, BlockID: _chunk.BlockID})
	}
	// 如果是覆盖，需要先删除旧的索引
	var _obj meta.Object
	exists, err := txn.Get(objKey, &_obj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s get object failed: %v", obj.Bucket, obj.Key, err)
		return fmt.Errorf("%s/%s get object failed: %v", obj.Bucket, obj.Key, err)
	}
	if exists {
		gckey := "aws:gc:chunks" + strings.ReplaceAll(uuid.New().String(), "-", "")
		err = txn.Set(gckey, &_obj.Chunks)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s set gc chunk failed: %v", obj.Bucket, obj.Key, err)
			return fmt.Errorf("%s/%s set gc chunk failed: %v", obj.Bucket, obj.Key, err)
		}
	}
	err = txn.Set(objKey, &obj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("set object %s/%s meta info failed: %v", obj.Bucket, obj.Key, err)
		return fmt.Errorf("set object %s/%s meta info failed: %v", obj.Bucket, obj.Key, err)
	} else {
		logger.GetLogger("boulder").Debugf("set object %s/%s meta  ok", obj.Bucket, obj.Key)
	}

	err = txn.Commit()
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s commit failed: %v", obj.Bucket, obj.Key, err)
		return fmt.Errorf("%s/%s commit failed: %v", obj.Bucket, obj.Key, err)
	}

	return nil
}
