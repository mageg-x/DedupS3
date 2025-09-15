package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	xcache "github.com/mageg-x/boulder/internal/storage/cache"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mageg-x/boulder/internal/utils"

	"github.com/mageg-x/boulder/service/storage"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/meta"
	"github.com/mageg-x/boulder/service/block"
)

const (
	// GCChunkPrefix GC前缀定义
	GCChunkPrefix = "aws:gc:chunks:"
	GCBlockPrefix = "aws:gc:blocks:"
	GCDedupPrefix = "aws:gc:dedup:"

	// DefaultScanInterval 扫描间隔默认值（秒）
	DefaultGCScanInterval = 10 * time.Second // 1分钟
)

var (
	ErrNoMoreData = errors.New("no more data")
)
var (
	gcInst *GCService
	gcMu   = sync.Mutex{}
)

type GCBlock struct {
	BlockID   string `json:"block_id" msgpack:"block_id"`
	StorageID string `json:"storage_id" msgpack:"storage_id"`
}

type GCChunk struct {
	StorageID string   `json:"storage_id" msgpack:"storage_id"`
	ChunkIDs  []string `json:"chunk_ids" msgpack:"chunk_ids"`
}

type GCDedup struct {
	BlockID   string
	StorageID string
}

type GCService struct {
	running atomic.Bool
	kvstore kv.KVStore
	mutex   sync.Mutex
}

// GetGCService 获取全局GC服务实例
func GetGCService() *GCService {
	gcMu.Lock()
	defer gcMu.Unlock()
	if gcInst != nil {
		return gcInst
	}
	logger.GetLogger("boulder").Infof("initializing garbage collection service")
	kvStore, err := kv.GetKvStore()
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get kv store for task: %v", err)
		return nil
	}
	gcInst = &GCService{
		kvstore: kvStore,
		running: atomic.Bool{},
		mutex:   sync.Mutex{},
	}
	logger.GetLogger("boulder").Infof("garbage collection service initialized successfully")

	return gcInst
}

// Start 启动垃圾回收服务
func (g *GCService) Start() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	if g.running.Load() {
		logger.GetLogger("boulder").Infof("garbage collection service is already running")
		return nil
	}

	g.running.Store(true)

	go g.loop()
	logger.GetLogger("boulder").Infof("garbage collection service started successfully")
	return nil
}

// Stop 停止垃圾回收服务
func (g *GCService) Stop() {
	g.mutex.Lock()
	g.mutex.Unlock()
	g.running.Store(false)
	logger.GetLogger("boulder").Infof("garbage collection service stopped successfully")
}

func (g *GCService) loop() {
	//g.checkBlock()
	for g.running.Load() {
		g.doClean() // 垃圾清理
		time.Sleep(DefaultGCScanInterval)
	}
}

func (g *GCService) doClean() {
	//dedup
	dedupCount, err := g.clean(GCDedupPrefix)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to clean up dedup: %v", err)
		return
	} else {
		logger.GetLogger("boulder").Tracef("cleaned up %d dedup", dedupCount)
	}

	// chunks
	chunkCount, err := g.clean(GCChunkPrefix)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to clean up chunks: %v", err)
		return
	} else {
		logger.GetLogger("boulder").Tracef("cleaned up %d chunks", chunkCount)
	}

	// 清理 blocks
	blockCount, err := g.clean(GCBlockPrefix)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to clean up blocks: %v", err)
		return
	} else {
		logger.GetLogger("boulder").Tracef("cleaned up %d blocks", blockCount)
	}

	logger.GetLogger("boulder").Tracef("garbage collection scan completed")
}

func (g *GCService) clean(prefix string) (int, error) {
	var cleanedCount int
	var lastErr error

	start := time.Now()
	logger.GetLogger("boulder").Errorf("garbage collection %s starting", prefix)
	defer func() {
		logger.GetLogger("boulder").Errorf("garbage collection %s completed in %s", prefix, time.Since(start))
	}()

	for {
		var count int
		finished := false
		// 单次扫描并处理一个 GC 条目
		switch prefix {
		case GCDedupPrefix:
			finished, count, _ = g.dedupOne4Block(prefix)
		case GCChunkPrefix:
			finished, count, _ = g.cleanOne4Chunk(prefix)
		case GCBlockPrefix:
			finished, count, _ = g.cleanOne4Block(prefix)
		default:
			logger.GetLogger("boulder").Debugf("cleaning up chunk prefix %s", prefix)
		}

		if finished {
			break
		}

		cleanedCount += count
	}
	return cleanedCount, lastErr
}

// cleanOne 处理一个 GC 条目（一个 keys[0] 对应的 chunkIDs）
func (g *GCService) cleanOne4Chunk(prefix string) (finished bool, count int, err error) {
	txn, err := g.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
		return false, 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 扫描一个前缀条目
	keys, _, err := txn.Scan(prefix, "", 1)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to scan keys: %v", err)
		return false, 0, fmt.Errorf("failed to scan: %w", err)
	}

	// 没有更多键了
	if len(keys) == 0 {
		//logger.GetLogger("boulder").Infof("no keys found, finish to task %s", prefix)
		return true, 0, nil
	} else {
		logger.GetLogger("boulder").Infof("found chunk %s to clean ", keys[0])
	}

	var gcChunks GCChunk
	exists, err := txn.Get(keys[0], &gcChunks)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get chunkids: %s  %v", keys[0], err)
		return false, 0, fmt.Errorf("failed to get chunkIDs from %s: %w", keys[0], err)
	}

	if !exists {
		logger.GetLogger("boulder").Infof("gcchunk %s not found ", keys[0])
		return false, 0, fmt.Errorf("gcchunk %s does not exist", keys[0])
	}

	blockMap := make(map[string]bool)
	var deletedCount int
	for _, chunkID := range gcChunks.ChunkIDs {
		chunkKey := meta.GenChunkKey(gcChunks.StorageID, chunkID)
		if cache, e := xcache.GetCache(); e == nil && cache != nil {
			_ = cache.Del(context.Background(), chunkKey)
		}
		var chunk meta.Chunk
		exists, err := txn.Get(chunkKey, &chunk)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get chunk %s: %v", chunkID, err)
			return false, 0, fmt.Errorf("failed to get chunk %s: %w", chunkKey, err)
		}

		if !exists {
			continue // 已经不存在，跳过
		}

		if chunk.RefCount > 1 {
			//logger.GetLogger("boulder").Debugf("chunk %s has ref more than one chunk", chunkID)
			chunk.RefCount--
			if err := txn.Set(chunkKey, &chunk); err != nil {
				logger.GetLogger("boulder").Errorf("failed to set chunk %s: %v", chunkID, err)
				return false, 0, fmt.Errorf("failed to update refCount for chunk %s: %w", chunkKey, err)
			}
		} else {
			if err := txn.Delete(chunkKey); err != nil {
				logger.GetLogger("boulder").Errorf("failed to delete chunk %s: %v", chunkID, err)
				return false, 0, fmt.Errorf("failed to delete chunk %s: %w", chunkKey, err)
			} else {
				logger.GetLogger("boulder").Infof("deleted chunk %s", chunkID)
				deletedCount++
				blockMap[chunk.BlockID] = true
			}
		}
	}

	// 删除 GC 记录本身
	if err := txn.Delete(keys[0]); err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete chunk %s: %v", keys[0], err)
		return false, 0, fmt.Errorf("failed to delete task key %s: %w", keys[0], err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit txn: %v", err)
		return false, 0, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = nil

	// 连带 block 也要清理
	if len(blockMap) > 0 {
		gcKey := GCBlockPrefix + utils.GenUUID()
		var gcBlocks []*GCBlock
		for blockID := range blockMap {
			gcBlocks = append(gcBlocks, &GCBlock{BlockID: blockID, StorageID: gcChunks.StorageID})
		}
		err := g.kvstore.Set(gcKey, gcBlocks)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to set gc blocks gcKey %s error : %v", gcKey, err)
		}
	}

	return false, deletedCount, nil
}

func (g *GCService) cleanOne4Block(prefix string) (finished bool, count int, err error) {
	txn, err := g.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
		return false, 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 扫描一个前缀条目
	keys, _, err := txn.Scan(prefix, "", 1)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to scan keys: %v", err)
		return false, 0, fmt.Errorf("failed to scan: %w", err)
	}

	// 没有更多键了
	if len(keys) == 0 {
		//logger.GetLogger("boulder").Infof("no keys found, finish to task %s", prefix)
		return true, 0, nil
	} else {
		logger.GetLogger("boulder").Infof("found gcblock %s to clean ", keys[0])
	}

	var gcBlocks []*GCBlock
	exists, err := txn.Get(keys[0], &gcBlocks)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get gcblockIDs: %s  %v", keys[0], err)
		return false, 0, fmt.Errorf("failed to get gcblockIDs from %s: %w", keys[0], err)
	}

	if !exists {
		logger.GetLogger("boulder").Infof("gcblock %s does not exist", keys[0])
		return false, 0, fmt.Errorf("gcblock %s does not exist", keys[0])
	}
	bs := block.GetBlockService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get block service")
		return false, 0, fmt.Errorf("failed to get block service")
	}

	var deletedCount int
	for _, gcBlock := range gcBlocks {
		blockKey := meta.GenBlockKey(gcBlock.StorageID, gcBlock.BlockID)
		if cache, e := xcache.GetCache(); e == nil && cache != nil {
			_ = cache.Del(context.Background(), blockKey)
		}
		var _block meta.Block
		exists, err := txn.Get(blockKey, &_block)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get gcblock %s: %v", blockKey, err)
			return false, 0, fmt.Errorf("failed to get gcblock %s: %w", blockKey, err)
		}
		if !exists {
			// 索引元数据都不存在，就直接删除实际数据了
			err := bs.RemoveBlock(gcBlock.StorageID, gcBlock.BlockID)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to remove gcblock %#v: %v", gcBlock, err)
				return false, 0, fmt.Errorf("failed to remove gcblock %#v: %w", gcBlock, err)
			} else {
				logger.GetLogger("boulder").Infof("removed block %#v", gcBlock)
			}
			deletedCount++
			continue
		}

		var chunkIDs []string
		for _, chunk := range _block.ChunkList {
			if chunk.Hash != meta.NONE_CHUNK_ID {
				_key := meta.GenChunkKey(gcBlock.StorageID, chunk.Hash)
				chunkIDs = append(chunkIDs, _key)
			}
		}

		// 看看是否已经没有任何索引关联
		canDel := true
		if len(chunkIDs) > 0 {
			chunks, err := txn.BatchGet(chunkIDs)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to batch get gcchunks: %v", err)
				return false, 0, fmt.Errorf("failed to batch get gcchunks: %w", err)
			}
			for k, v := range chunks {
				if v == nil {
					continue
				}

				var _chunk meta.Chunk
				// 把 []byte 反序列化为 meta.Chunk
				if err := json.Unmarshal(v, &_chunk); err != nil {
					logger.GetLogger("boulder").Errorf("failed to unmarshal chunk %s: %v", k, err)
					canDel = false
					break
				}

				// 现在可以检查 RefCount
				if _chunk.RefCount > 0 {
					logger.GetLogger("boulder").Warnf("cannot delete chunk %s: refcount = %d", k, _chunk.RefCount)
					canDel = false
					break
				} else {
					// 顺手清理掉 引用为 0 的元数据
					_key := meta.GenChunkKey(gcBlock.StorageID, k)
					err := txn.Delete(_key)
					if err != nil {
						logger.GetLogger("boulder").Errorf("failed to delete chunk %s: %v", k, err)
					} else {
						logger.GetLogger("boulder").Infof("deleted ref = 0 chunk %s", _key)
					}
				}
			}
		}

		if canDel {
			// 先删除索引元数据
			err := txn.Delete(blockKey)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to delete block %s: %v", blockKey, err)
				return false, 0, fmt.Errorf("failed to delete chunk %s: %w", blockKey, err)
			}
			// 再删除实际数据
			err = bs.RemoveBlock(gcBlock.StorageID, gcBlock.BlockID)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to remove block %s: %v", gcBlock.BlockID, err)
				return false, 0, fmt.Errorf("failed to remove block %s: %w", gcBlock.BlockID, err)
			}

			deletedCount++
		}
	}

	// 删除 GC 记录本身
	if err := txn.Delete(keys[0]); err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete block %s: %v", keys[0], err)
		return false, 0, fmt.Errorf("failed to delete task key %s: %w", keys[0], err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit txn: %v", err)
		return false, 0, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = nil
	return false, deletedCount, nil
}

func (g *GCService) dedupOne4Block(prefix string) (finished bool, count int, err error) {
	start := time.Now()
	logger.GetLogger("boulder").Errorf("start dedup one 4 block %s", prefix)
	defer func() {
		logger.GetLogger("boulder").Errorf("finished dedup one 4 block %s in %v", prefix, time.Since(start))
	}()

	bs := block.GetBlockService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get block service")
		return false, 0, fmt.Errorf("failed to get block service")
	}

	txn, err := g.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
		return false, 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 扫描一个前缀条目
	keys, _, err := txn.Scan(prefix, "", 1)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to scan keys: %v", err)
		return false, 0, fmt.Errorf("failed to scan: %w", err)
	}

	// 没有更多键了
	if len(keys) == 0 {
		//logger.GetLogger("boulder").Infof("no keys found, finish to task %s", prefix)
		return true, 0, nil
	} else {
		logger.GetLogger("boulder").Infof("found chunk %s to clean ", keys[0])
	}

	var gcDedup GCDedup
	exists, err := txn.Get(keys[0], &gcDedup)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get gcDedup: %s  %v", keys[0], err)
		return false, 0, fmt.Errorf("failed to get gcDedup from %s: %w", keys[0], err)
	}

	if !exists {
		logger.GetLogger("boulder").Infof("gcDedup %s not found ", keys[0])
		return false, 0, fmt.Errorf("gcDedup %s does not exist", keys[0])
	}

	_block := meta.Block{}
	blockKey := meta.GenBlockKey(gcDedup.StorageID, gcDedup.BlockID)
	exists, err = txn.Get(blockKey, &_block)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get block %s: %v", blockKey, err)
		return false, 0, fmt.Errorf("failed to get block %s: %w", blockKey, err)
	}
	if !exists {
		logger.GetLogger("boulder").Errorf("block %s not found ", blockKey)
	}

	var deletedCount int

	if _block.Finally {
		chunks := make(map[string]meta.BlockChunk, 0)

		for _, _ck := range _block.ChunkList {
			if _ck.Hash != meta.NONE_CHUNK_ID {
				chunks[_ck.Hash] = _ck
			}
		}

		if len(chunks) == 0 {
			// 可以直接删除 block 了
			deletedCount = len(_block.ChunkList)
			gcKey := GCBlockPrefix + utils.GenUUID()
			var gcBlocks []*GCBlock
			gcBlocks = append(gcBlocks, &GCBlock{BlockID: _block.ID, StorageID: _block.StorageID})
			err = txn.Set(gcKey, gcBlocks)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to set gcBlocks: %v", err)
				return false, 0, fmt.Errorf("failed to set gcBlocks: %w", err)
			}
		} else if len(_block.ChunkList) == len(chunks) {
			// 无去重内容，不需要处理
		} else {
			bs := block.GetBlockService()
			if bs == nil {
				logger.GetLogger("boulder").Errorf("failed to get block service")
				return false, 0, fmt.Errorf("failed to get block service")
			}

			blockData, err := bs.ReadBlock(_block.StorageID, _block.ID)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to read block data: %v", err)
				return false, 0, fmt.Errorf("failed to read block data: %w", err)
			}
			deletedCount = len(blockData.ChunkList) - len(chunks)

			newBlockData := &meta.BlockData{
				BlockHeader: blockData.BlockHeader,
				Data:        make([]byte, 0, _block.TotalSize),
			}
			newBlockData.ChunkList = make([]meta.BlockChunk, 0)
			newBlockData.TotalSize = 0
			offset := int32(0)

			for _, _ck := range blockData.ChunkList {
				if _, ok := chunks[_ck.Hash]; ok {
					newBlockData.ChunkList = append(newBlockData.ChunkList, _ck)
					newBlockData.Data = append(newBlockData.Data, blockData.Data[offset:offset+_ck.Size]...)
					newBlockData.TotalSize += int64(_ck.Size)
				} else {
					logger.GetLogger("boulder").Errorf("block chunk %s/ %s is noref chunk id", _block.ID, _ck.Hash)
				}
				offset += _ck.Size
			}

			err = bs.WriteBlock(_block.StorageID, newBlockData)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to write block data: %v", err)
				return false, 0, fmt.Errorf("failed to write block data: %w", err)
			}
			_block.BlockHeader = newBlockData.BlockHeader
			copy(_block.ChunkList, newBlockData.ChunkList)

			err = txn.Set(blockKey, &_block)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to set block %s meta: %v", _block.ID, err)
				return false, 0, fmt.Errorf("failed to set block %s meta: %w", _block.ID, err)
			}
		}
	}

	// 删除 GC 记录本身
	if err := txn.Delete(keys[0]); err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete block %s: %v", keys[0], err)
		return false, 0, fmt.Errorf("failed to delete task key %s: %w", keys[0], err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit txn: %v", err)
		return false, 0, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = nil
	return false, deletedCount, nil
}

func (g *GCService) checkBlock() {
	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("boulder").Errorf("failed to get storage service")
		return
	}

	stores := ss.GetStoragesByClass("STANDARD")
	if len(stores) == 0 {
		logger.GetLogger("boulder").Errorf("failed to find stores")
		return
	}

	s, err := ss.GetStorage(stores[0].ID)
	if s == nil || err != nil || s.Instance == nil {
		logger.GetLogger("boulder").Errorf("failed to find store")
		return
	}

	bs := block.GetBlockService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get block service")
		return
	}

	chunks := make(map[string]string, 0)
	chunk2block := make(map[string]string, 0)

	blockCh, errCh := s.Instance.List()
	for blockID := range blockCh {
		_blockData, err := bs.ReadBlock(s.ID, blockID)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to read block %s: %v", blockID, err)
			continue
		}
		logger.GetLogger("boulder").Errorf("block data header %#v", _blockData.BlockHeader)

		blockKey := meta.GenBlockKey(s.ID, blockID)
		var _blockMeta meta.Block
		exists, err := g.kvstore.Get(blockKey, &_blockMeta)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get block meta %s: %v", blockKey, err)
		}
		if !exists {
			logger.GetLogger("boulder").Errorf("block meta %s not exists", blockKey)
		}

		logger.GetLogger("boulder").Errorf("block meta header %#v", _blockMeta.BlockHeader)

		logger.GetLogger("boulder").Infof("read block %s chunk %#v", blockID, len(_blockData.ChunkList))
		for _, chunk := range _blockData.ChunkList {
			if bid, ok := chunks[chunk.Hash]; ok {
				logger.GetLogger("boulder").Errorf("chunk %s has exists in %s : %s , %v", chunk.Hash, blockID, bid, _blockData.Finally)
				var _chunkMeta meta.Chunk
				chunkKey := meta.GenChunkKey(s.ID, chunk.Hash)
				exists, err := g.kvstore.Get(chunkKey, &_chunkMeta)
				if err == nil && exists {
					logger.GetLogger("boulder").Errorf("get chunk meta %#v", _chunkMeta)
				}
			} else {
				chunks[chunk.Hash] = blockID
			}

			if chunk2block[chunk.Hash] != "" {
				logger.GetLogger("boulder").Infof("block %s has chunk %s", blockID, chunk.Hash)
			}

			chunk2block[chunk.Hash] = blockID
		}
	}

	// 检查是否因错误提前结束
	if err := <-errCh; err != nil {
		logger.GetLogger("boulder").Errorf("failed to list blocks: %v", err)
		return
	}
}
