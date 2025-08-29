package chunk

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mageg-x/boulder/service/task"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/service/block"

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
		chunks, err := c.Dedup(ctx, chunkChan, dedupChan, obj)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s merge chunk failed: %v", obj.Bucket, obj.Key, err)
			cancel()
			return
		}
		allChan <- chunks
	}()

	rollback := false
	//重组
	blocks, err := c.Assemble(ctx, dedupChan, obj.DataLocation, obj.Bucket, obj.Key)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s assemble chunk failed: %v", obj.Bucket, obj.Key, err)
		rollback = true
		cancel()
	}

	// 汇总信息
	allChunk, err := c.Summary(ctx, allChan, blocks, obj.Bucket, obj.Key)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s summary chunk failed: %v", obj.Bucket, obj.Key, err)
		rollback = true
		cancel()
	} else {
		//logger.GetLogger("boulder").Debugf("%s/%s summary chunk finished %+v", obj.Bucket, obj.Key, allChunk)
	}

	if !rollback {
		var txErr error
		maxRetry := 3
		// 重试三次， 在文件相同时候，并发上传，会造成 事务冲突
		for i := 0; i < maxRetry; i++ {
			// 备份 allchunk, blocks, obj
			bakAllChunks := make([]*meta.Chunk, 0, len(allChunk))
			for _, ck := range allChunk {
				newChunk := ck.Clone()
				bakAllChunks = append(bakAllChunks, newChunk)
			}

			bakBlocks := make(map[string]*meta.Block, len(blocks))
			for k, v := range blocks {
				newBlock := v.Clone()
				bakBlocks[k] = newBlock
			}

			bakObj := obj.Clone()

			txErr = c.WriteMeta(ctx, obj.Owner.ID, bakAllChunks, bakBlocks, bakObj)
			if txErr == nil {
				break
			} else if errors.Is(txErr, kv.ErrTxnCommit) {
				// 事务提交冲突
				logger.GetLogger("boulder").Errorf("%s/%s commit failed: %v", obj.Bucket, obj.Key, txErr)
				if i < maxRetry-1 {
					baseDelay := 500 * time.Millisecond
					jitter := time.Duration(rand.Int63n(100)) * time.Millisecond
					sleep := baseDelay<<uint(i) + jitter
					time.Sleep(sleep)
				}
			} else {
				logger.GetLogger("boulder").Errorf("%s/%s write meta info failed: %v", obj.Bucket, obj.Key, txErr)
			}
		}

		if txErr != nil {
			rollback = true
			cancel()
		}
	}

	// 回滚处理，删除之前写入的block
	if rollback {
		gcKey := task.GCBlockPrefix + utils.GenUUID()
		var gcBlocks []*task.GCBlock
		for _, _block := range blocks {
			gcBlocks = append(gcBlocks, &task.GCBlock{BlockID: _block.ID, StorageID: obj.DataLocation})
		}
		logger.GetLogger("boulder").Warnf("rollback for %s/%s delete blocks %#v ", obj.Bucket, obj.Key, gcBlocks)
		err := c.kvstore.Set(gcKey, &gcBlocks)
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
	var prevChunk *meta.Chunk
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
			currentChunk := meta.NewChunk(chunkData)
			// 避免最后一个分片太小
			if err == io.EOF && len(chunkData) < opt.MinSize && prevChunk != nil {
				// 合并到前一个分片
				mergedData := append(prevChunk.Data, chunkData...)
				prevChunk = meta.NewChunk(mergedData)
			} else {
				// 发送前一个分片（如果有）
				if prevChunk != nil {
					outputChan <- prevChunk
				}
				// 当前分片成为新的前一个分片
				prevChunk = currentChunk
			}
		}

		// 检查是否结束
		if err == io.EOF {
			break
		}
	}
	// 发送最后一个分片（可能是合并后的）
	if prevChunk != nil {
		outputChan <- prevChunk
	}
	return nil
}

func (c *ChunkService) Dedup(ctx context.Context, inputChan, dedupChan chan *meta.Chunk, obj *meta.Object) ([]*meta.Chunk, error) {
	allChan := make([]*meta.Chunk, 0)
	chunkFilter := make(map[string]string)
	// 初始化 MD5 哈希用于计算整个数据块的MD5
	fullMD5 := md5.New()
	// 批量处理
	batchDedup := make([]*meta.Chunk, 0)
	finished := false
	for !finished {
		select {
		case <-ctx.Done():
			// 超时或取消，退出循环
			logger.GetLogger("boulder").Warnf("chunking timed out or cancelled: %v", ctx.Err())
			return nil, fmt.Errorf("chunking timed out or cancelled for %s/%s: %v", obj.Bucket, obj.Key, ctx.Err())
		case chunk, ok := <-inputChan:
			if ok && chunk != nil {
				//logger.GetLogger("boulder").Debugf("get cdc chunk %d fp %s", chunk.Size, chunk.Hash)
				// 更新整个数据块的 MD5
				fullMD5.Write(chunk.Data)
				allChan = append(allChan, chunk)
				batchDedup = append(batchDedup, chunk)
			} else {
				// 通道已关闭，退出循环
				finished = true
			}

			if len(batchDedup) > 100 || (finished && len(batchDedup) > 0) {
				keys := make([]string, 0)
				for _, item := range batchDedup {
					chunkKey := "aws:chunk:" + item.Hash
					keys = append(keys, chunkKey)
				}
				_chunks, err := c.kvstore.BatchGet(keys)
				if err != nil {
					logger.GetLogger("boulder").Errorf("%s/%s batch get failed: %v", obj.Bucket, obj.Key, err)
					return nil, fmt.Errorf("%s/%s batch get failed: %v", obj.Bucket, obj.Key, err)
				}

				for _, item := range batchDedup {
					// 对象间去重
					chunkKey := "aws:chunk:" + item.Hash
					if _chunks[chunkKey] != nil {
						var _chunk meta.Chunk
						err = json.Unmarshal(_chunks[chunkKey], &_chunk)
						if err != nil {
							logger.GetLogger("boulder").Errorf("%s/%s chunk unmarshal failed: %v", obj.Bucket, obj.Key, err)
							return nil, fmt.Errorf("%s/%s chunk unmarshal failed: %v", obj.Bucket, obj.Key, err)
						}
						chunkFilter[item.Hash] = _chunk.BlockID
						item.BlockID = _chunk.BlockID
						item.Data = nil
						logger.GetLogger("boulder").Debugf("chunk %s/%s/%s has already been dedupped  in block %#v between object", obj.Bucket, obj.Key, item.Hash, _chunk)
						continue
					}

					// 对象内去重
					if chunkFilter[item.Hash] != "" {
						item.BlockID = chunkFilter[item.Hash]
						item.Data = nil
						logger.GetLogger("boulder").Debugf("chunk %s/%s/%s has already been dedupped inner object", obj.Bucket, obj.Key, item.Hash)
						continue
					}

					logger.GetLogger("boulder").Warnf("chunk %s/%s/%s has not found dedupped", obj.Bucket, obj.Key, item.Hash)
					chunkFilter[item.Hash] = "000000000000000000000000"

					dedupChan <- item
				}
				batchDedup = make([]*meta.Chunk, 0)
			}
		}
	}
	// 计算整个数据块的 MD5
	fullMD5Sum := fullMD5.Sum(nil)
	fullMD5Hex := hex.EncodeToString(fullMD5Sum)
	obj.ETag = fullMD5Hex
	logger.GetLogger("boulder").Infof("%s/%s full data md5: %s", obj.Bucket, obj.Key, fullMD5Hex)
	return allChan, nil
}

func (c *ChunkService) Assemble(ctx context.Context, dedupChan chan *meta.Chunk, storageID, bucket, objkey string) (map[string]*meta.Block, error) {
	blocks := make(map[string]*meta.Block)
	finished := false
	for !finished {
		select {
		case <-ctx.Done():
			logger.GetLogger("boulder").Warnf(" assemble chunk be cancelled: %v", ctx.Err())
			return blocks, fmt.Errorf("assemble chunk be cancelled for %s/%s: %v", bucket, objkey, ctx.Err())
		case chunk, ok := <-dedupChan:
			bs := block.GetBlockService()
			if bs == nil {
				logger.GetLogger("boulder").Errorf("%s/%s block service is nil", bucket, objkey)
				return blocks, fmt.Errorf("%s/%s block service is nil", bucket, objkey)
			}

			_block, err := bs.PutChunk(chunk, storageID, bucket, objkey)
			if err != nil {
				logger.GetLogger("boulder").Errorf("%s/%s put chunk failed: %v", bucket, objkey, err)
				return blocks, fmt.Errorf("%s/%s put chunk failed: %v", bucket, objkey, err)
			}

			if _block != nil {
				blocks[_block.ID] = _block
			}

			if !ok || chunk == nil {
				finished = true
				logger.GetLogger("boulder").Warnf("block service is finish assemble object %s/%s  ", bucket, objkey)
				break
			}
		}
	}
	return blocks, nil
}

func (c *ChunkService) Summary(ctx context.Context, allChan chan []*meta.Chunk, blocks map[string]*meta.Block, bucket, objkey string) ([]*meta.Chunk, error) {
	allChunks := make([]*meta.Chunk, 0)
	chunk2block := make(map[string]string)
	for _, _block := range blocks {
		for _, _chunk := range _block.ChunkList {
			chunk2block[_chunk.Hash] = _block.ID
			//logger.GetLogger("boulder").Debugf(" chunk ----> block : %s--->%s", _chunk.Hash, _block.ID)
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
			//logger.GetLogger("boulder").Debugf("chunk %s block id %s:%s", chunk.Hash, chunk.BlockID, chunk2block[chunk.Hash])
			if chunk2block[chunk.Hash] != "" {
				chunk.BlockID = chunk2block[chunk.Hash]
			}

			if chunk.BlockID == "" {
				logger.GetLogger("boulder").Errorf("%s/%s/%s dedup chunk failed", bucket, objkey, chunk.Hash)
				return allChunks, fmt.Errorf("%s/%s dedup chunk failed", bucket, objkey)
			}
			chunk.Data = nil
			allChunks = append(allChunks, chunk)
		}
		break
	}
	return allChunks, nil
}

func (c *ChunkService) WriteMeta(ctx context.Context, accountID string, allChunk []*meta.Chunk, blocks map[string]*meta.Block, obj *meta.Object) error {
	txn, err := c.kvstore.BeginTxn(ctx, nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s create transaction failed: %v", obj.Bucket, obj.Key, err)
		return fmt.Errorf("%s/%s create transaction failed: %v", obj.Bucket, obj.Key, err)
	}
	defer txn.Rollback()
	type Pair struct {
		BlockID string
		ChunkID string
	}

	fixed := make(map[string]*Pair)

	// 写入chunk元数据
	for _, chunk := range allChunk {
		//logger.GetLogger("boulder").Errorf("%s/%s write chunk %s:%s:%d ", obj.Bucket, obj.Key, chunk.Hash, chunk.BlockID, len(chunk.Data))
		chunkey := "aws:chunk:" + chunk.Hash
		var _chunk meta.Chunk
		exists, e := txn.Get(chunkey, &_chunk)
		if e != nil {
			logger.GetLogger("boulder").Errorf("%s/%s get chunk failed: %v", obj.Bucket, obj.Key, err)
			return fmt.Errorf("%s/%s get chunk %s failed: %v", obj.Bucket, obj.Key, chunkey, err)
		}

		if exists {
			// 这里要检查 同一个 chunk 存在多个重复存放问题
			if _chunk.BlockID != chunk.BlockID {
				logger.GetLogger("boulder").Warnf("%s/%s  chunk %s has multi bolock %s:%s", obj.Bucket, obj.Key, chunk.Hash, _chunk.BlockID, chunk.BlockID)
				fixed[chunk.BlockID+":"+chunk.Hash] = &Pair{BlockID: _chunk.BlockID, ChunkID: chunk.Hash}
				chunk.BlockID = _chunk.BlockID
			}

			_chunk.RefCount += 1
			e := txn.Set(chunkey, &_chunk)
			if e != nil {
				logger.GetLogger("boulder").Errorf("%s/%s set chunk %s failed: %v", obj.Bucket, obj.Key, _chunk.Hash, err)
				return fmt.Errorf("%s/%s set chunk failed: %v", obj.Bucket, obj.Key, err)
			} else {
				logger.GetLogger("boulder").Debugf("%s/%s refresh set chunk: %s", obj.Bucket, obj.Key, _chunk.Hash)
			}
		} else {
			chunk.RefCount = 1
			e := txn.Set(chunkey, &chunk)
			if e != nil {
				logger.GetLogger("boulder").Errorf("%s/%s set chunk %s failed: %v", obj.Bucket, obj.Key, chunkey, err)
				return fmt.Errorf("%s/%s set chunk failed: %v", obj.Bucket, obj.Key, err)
			} else {
				logger.GetLogger("boulder").Debugf("%s/%s add  chunk: %s", obj.Bucket, obj.Key, chunk.Hash)
			}
		}
	}

	// 写入block的元数据
	for _, item := range blocks {
		//logger.GetLogger("boulder").Errorf("%s/%s write block meta inf : %s:%d:%d", obj.Bucket, obj.Key, item.ID, item.TotalSize, item.RealSize)
		blockKey := "aws:block:" + item.ID
		var newChunkList []meta.BlockChunk
		for _, _chunk := range item.ChunkList {
			k := item.ID + ":" + _chunk.Hash
			if fixed[k] == nil {
				newChunkList = append(newChunkList, _chunk)
			} else {
				logger.GetLogger("boulder").Warnf("remove refrence error chunk %s/%s/%s", obj.Bucket, obj.Key, k)
			}
		}
		item.ChunkList = newChunkList

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
		k := _chunk.BlockID + ":" + _chunk.Hash
		if fixed[k] != nil {
			logger.GetLogger("boulder").Warnf("fix refrence error chunk %s/%s/%s", obj.Bucket, obj.Key, k)
			obj.Chunks = append(obj.Chunks, meta.ObjChunk{Hash: fixed[k].ChunkID, BlockID: fixed[k].BlockID})
		} else {
			obj.Chunks = append(obj.Chunks, meta.ObjChunk{Hash: _chunk.Hash, BlockID: _chunk.BlockID})
		}
	}
	//logger.GetLogger("boulder").Errorf("%s/%s write object meta %#v", obj.Bucket, obj.Key, obj)
	// 如果是覆盖，需要先删除旧的索引
	var _obj meta.Object
	exists, err := txn.Get(objKey, &_obj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s get object failed: %v", obj.Bucket, obj.Key, err)
		return fmt.Errorf("%s/%s get object failed: %v", obj.Bucket, obj.Key, err)
	}
	if exists {
		var chunkIDs []string
		for _, chunk := range _obj.Chunks {
			chunkIDs = append(chunkIDs, chunk.Hash)
		}

		gckey := task.GCChunkPrefix + utils.GenUUID()
		err = txn.Set(gckey, &chunkIDs)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s set task chunk failed: %v", obj.Bucket, obj.Key, err)
			return fmt.Errorf("%s/%s set task chunk failed: %v", obj.Bucket, obj.Key, err)
		} else {
			logger.GetLogger("boulder").Infof("%s/%s set gc chunk %s delay to proccess: %v", obj.Bucket, obj.Key, gckey, err)
		}
	}
	logger.GetLogger("boulder").Infof("set object %s meta ..... ", objKey)
	err = txn.Set(objKey, obj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("set object %s/%s meta info failed: %v", obj.Bucket, obj.Key, err)
		return fmt.Errorf("set object %s/%s meta info failed: %v", obj.Bucket, obj.Key, err)
	} else {
		logger.GetLogger("boulder").Debugf("set object %s/%s meta  ok", obj.Bucket, obj.Key)
	}

	err = txn.Commit()
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s commit failed: %v", obj.Bucket, obj.Key, err)
		return kv.ErrTxnCommit
	}

	return nil
}
