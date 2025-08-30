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
		err := c.Split(ctx, r, chunkChan, opts, obj)
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
	blocks, err := c.Assemble(ctx, dedupChan, obj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s assemble chunk failed: %v", obj.Bucket, obj.Key, err)
		rollback = true
		cancel()
	}

	// 汇总信息
	allChunk, err := c.Summary(ctx, allChan, blocks, obj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s summary chunk failed: %v", obj.Bucket, obj.Key, err)
		rollback = true
		cancel()
	} else {
		logger.GetLogger("boulder").Debugf("%s/%s summary chunk finished %+v", obj.Bucket, obj.Key, allChunk)
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
			} else if errors.Is(txErr, kv.ErrTxnCommit) && i < maxRetry-1 {
				// 事务提交冲突
				logger.GetLogger("boulder").Warnf("transmission write object %s/%s commit failed: %v, and  retry %d times", obj.Bucket, obj.Key, txErr, i+1)
				baseDelay := 500 * time.Millisecond
				jitter := time.Duration(rand.Int63n(100)) * time.Millisecond
				sleep := baseDelay<<uint(i) + jitter
				time.Sleep(sleep)
			} else {
				logger.GetLogger("boulder").Errorf("transmission write object %s/%s  meta info failed: %v，retry times %d", obj.Bucket, obj.Key, txErr, i+1)
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
		return fmt.Errorf("dochunk %s/%s failed, then rollback block data", obj.Bucket, obj.Key)
	}

	return nil
}

// Split DoChunk 简单的CDC分块函数
func (c *ChunkService) Split(ctx context.Context, r io.Reader, outputChan chan *meta.Chunk, opt *fastcdc.ChunkerOpts, obj *meta.Object) error {
	// 创建CDC分块器
	chunker, err := fastcdc.NewChunker("fastcdc", r, opt)
	if err != nil {
		return fmt.Errorf("error creating chunker: %v", err)
	}
	var prevChunk *meta.Chunk

	objSize := 0 // 统计真实长度
	// 循环读取并分块
	for {
		// 检查 context 是否已取消
		select {
		case <-ctx.Done():
			return fmt.Errorf("fast cdc %s/%s be canceled: %v", obj.Bucket, obj.Key, ctx.Err())
		default:
			// 继续执行
		}

		chunkData, err := chunker.Next()
		if err != nil && err != io.EOF {
			return fmt.Errorf("cdc chunk %s/%s error: %v", obj.Bucket, obj.Key, err)
		}

		objSize += len(chunkData)

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
	logger.GetLogger("boulder").Infof("split object %s/%s  to chunk finished, get size %d:%d", obj.Bucket, obj.Key, objSize, obj.Size)
	obj.Size = int64(objSize)
	return nil
}

func (c *ChunkService) Dedup(ctx context.Context, inputChan, dedupChan chan *meta.Chunk, obj *meta.Object) ([]*meta.Chunk, error) {
	allChunk := make([]*meta.Chunk, 0)
	chunkFilter := make(map[string]string)
	// 初始化 MD5 哈希用于计算整个数据块的MD5
	fullMD5 := md5.New()
	// 批量处理
	batchDedup := make([]*meta.Chunk, 0)
	finished := false
	dedupNum := 0
	offset := 0
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
				allChunk = append(allChunk, chunk)
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
					offset += int(item.Size)
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
						logger.GetLogger("boulder").Infof("chunk %s/%s/%s [%d-%d] has already been dedupped in block %s between object", obj.Bucket, obj.Key, item.Hash, offset-int(item.Size), offset, _chunk.BlockID)
						item.BlockID = _chunk.BlockID
						item.Data = nil
						dedupNum++
						continue
					}

					// 对象内去重
					if chunkFilter[item.Hash] != "" {
						logger.GetLogger("boulder").Infof("chunk %s/%s/%s [%d-%d] has already been dedupped inner object", obj.Bucket, obj.Key, item.Hash, offset-int(item.Size), offset)
						item.BlockID = chunkFilter[item.Hash]
						item.Data = nil
						dedupNum++
						continue
					}

					logger.GetLogger("boulder").Infof("chunk %s/%s/%s has not found dedupped", obj.Bucket, obj.Key, item.Hash)
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
	logger.GetLogger("boulder").Infof("dedump object %s/%s finished, md5 is %s  all chunk num is %d dedup chunk num is %d", obj.Bucket, obj.Key, fullMD5Hex, len(allChunk), dedupNum)
	return allChunk, nil
}

func (c *ChunkService) Assemble(ctx context.Context, dedupChan chan *meta.Chunk, obj *meta.Object) (map[string]*meta.Block, error) {
	blocks := make(map[string]*meta.Block)
	finished := false
	blockNum := 0
	for !finished {
		select {
		case <-ctx.Done():
			logger.GetLogger("boulder").Warnf(" assemble chunk be cancelled: %v", ctx.Err())
			return blocks, fmt.Errorf("assemble chunk be cancelled for %s/%s: %v", obj.Bucket, obj.Key, ctx.Err())
		case chunk, ok := <-dedupChan:
			bs := block.GetBlockService()
			if bs == nil {
				logger.GetLogger("boulder").Errorf("%s/%s block service is nil", obj.Bucket, obj.Key)
				return blocks, fmt.Errorf("%s/%s block service is nil", obj.Bucket, obj.Key)
			}

			_block, err := bs.PutChunk(chunk, obj)
			if err != nil {
				logger.GetLogger("boulder").Errorf("%s/%s put chunk failed: %v", obj.Bucket, obj.Key, err)
				return blocks, fmt.Errorf("%s/%s put chunk failed: %v", obj.Bucket, obj.Key, err)
			}

			if _block != nil {
				blockNum++
				blocks[_block.ID] = _block
			}

			if !ok || chunk == nil {
				finished = true
				logger.GetLogger("boulder").Infof("block service is finish assemble object %s/%s", obj.Bucket, obj.Key)
				break
			}
		}
	}
	logger.GetLogger("boulder").Infof("assemble object %s/%s finished, get block num %d", obj.Bucket, obj.Key, blockNum)
	return blocks, nil
}

// Summary 主要用途是 修正 chunk 里面的blockid 索引信息, 统计 object 的实际大小
func (c *ChunkService) Summary(ctx context.Context, allChan chan []*meta.Chunk, blocks map[string]*meta.Block, obj *meta.Object) ([]*meta.Chunk, error) {
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
		logger.GetLogger("boulder").Errorf("%s/%s do chunk be canceled", obj.Bucket, obj.Key)
		return allChunks, fmt.Errorf("%s/%s do chunk be canceled", obj.Bucket, obj.Key)
	case result := <-allChan:
		if result == nil {
			logger.GetLogger("boulder").Errorf("%s/%s dedup chunk failed", obj.Bucket, obj.Key)
			return allChunks, fmt.Errorf("%s/%s dedup chunk failed", obj.Bucket, obj.Key)
		}
		for _, chunk := range result {
			if chunk == nil {
				logger.GetLogger("boulder").Errorf("%s/%s dedup chunk failed", obj.Bucket, obj.Key)
				return allChunks, fmt.Errorf("%s/%s dedup chunk failed", obj.Bucket, obj.Key)
			}
			//logger.GetLogger("boulder").Debugf("chunk %s block id %s:%s", chunk.Hash, chunk.BlockID, chunk2block[chunk.Hash])
			if chunk2block[chunk.Hash] != "" {
				chunk.BlockID = chunk2block[chunk.Hash]
			}

			if chunk.BlockID == "" {
				logger.GetLogger("boulder").Errorf("%s/%s/%s dedup chunk failed", obj.Bucket, obj.Key, chunk.Hash)
				return allChunks, fmt.Errorf("%s/%s dedup chunk failed", obj.Bucket, obj.Key)
			}
			chunk.Data = nil
			allChunks = append(allChunks, chunk)
		}
		break
	}

	logger.GetLogger("boulder").Infof("object summary %s/%s finished", obj.Bucket, obj.Key)
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
		obj.Chunks = append(obj.Chunks, _chunk.Hash)
	}
	//logger.GetLogger("boulder").Errorf("%s/%s write object meta %#v", obj.Bucket, obj.Key, obj)
	// 如果是覆盖，需要先删除旧的索引
	var _obj meta.Object
	exists, err := txn.Get(objKey, &_obj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s get object failed: %v", obj.Bucket, obj.Key, err)
		return fmt.Errorf("%s/%s get object failed: %v", obj.Bucket, obj.Key, err)
	}
	if exists && len(_obj.Chunks) > 0 {
		gckey := task.GCChunkPrefix + utils.GenUUID()
		err = txn.Set(gckey, &_obj.Chunks)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s set task chunk failed: %v", obj.Bucket, obj.Key, err)
			return fmt.Errorf("%s/%s set task chunk failed: %v", obj.Bucket, obj.Key, err)
		} else {
			logger.GetLogger("boulder").Infof("%s/%s set gc chunk %s delay to proccess", obj.Bucket, obj.Key, gckey)
		}
	}
	logger.GetLogger("boulder").Infof("set object %s etag %s , put meta ..... ", objKey, obj.ETag)
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
	logger.GetLogger("boulder").Infof("write object %s/%s  all meta data finish", obj.Bucket, obj.Key)
	return nil
}
