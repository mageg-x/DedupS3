package task

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mageg-x/boulder/service/storage"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/meta"
	"github.com/mageg-x/boulder/service/block"
)

const (
	// GCChunkPrefix GC前缀定义
	GCChunkPrefix = "aws:gc:chunks:"
	GCBlockPrefix = "aws:gc:blocks:"

	// DefaultScanInterval 扫描间隔默认值（秒）
	DefaultScanInterval = 10 // 1分钟
)

var (
	instance *GCService
	mu       = sync.Mutex{}
)

type GCBlock struct {
	BlockID   string `json:"block_id" msgpack:"block_id"`
	StorageID string `json:"storage_id" msgpack:"storage_id"`
}

type GCService struct {
	running atomic.Bool
	kvstore kv.KVStore
	mutex   sync.Mutex
}

// GetGCService 获取全局GC服务实例
func GetGCService() *GCService {
	mu.Lock()
	defer mu.Unlock()
	if instance != nil {
		return instance
	}
	logger.GetLogger("boulder").Infof("initializing garbage collection service")
	kvStore, err := kv.GetKvStore()
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get kv store for task: %v", err)
		return nil
	}
	instance = &GCService{
		kvstore: kvStore,
		running: atomic.Bool{},
		mutex:   sync.Mutex{},
	}
	logger.GetLogger("boulder").Infof("garbage collection service initialized successfully")

	return instance
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
	for g.running.Load() {
		g.doGC()
		//g.checkBlock()
		time.Sleep(DefaultScanInterval * time.Second)
	}
}

func (g *GCService) doGC() {
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

	for {
		var err error
		var count int
		// 单次扫描并处理一个 GC 条目
		switch prefix {
		case GCChunkPrefix:
			count, err = g.cleanOne4Chunk(prefix)
		case GCBlockPrefix:
			count, err = g.cleanOne4Block(prefix)
		default:
			logger.GetLogger("boulder").Debugf("cleaning up chunk prefix %s", prefix)
		}

		if err != nil {
			lastErr = err
			break
		}

		if count == 0 {
			// 没有更多数据可处理
			break
		}

		cleanedCount += count
	}

	return cleanedCount, lastErr
}

// cleanOne 处理一个 GC 条目（一个 keys[0] 对应的 chunkIDs）
func (g *GCService) cleanOne4Chunk(prefix string) (int, error) {
	txn, err := g.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer txn.Rollback()

	// 扫描一个前缀条目
	keys, _, err := txn.Scan(prefix, "", 1)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to scan keys: %v", err)
		return 0, fmt.Errorf("failed to scan: %w", err)
	}

	// 没有更多键了
	if len(keys) == 0 {
		//logger.GetLogger("boulder").Infof("no keys found, finish to task %s", prefix)
		return 0, nil
	} else {
		logger.GetLogger("boulder").Infof("found %s to clean ", keys[0])
	}

	var chunkIDs []string
	exists, err := txn.Get(keys[0], &chunkIDs)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get chunkids: %s  %v", keys[0], err)
		return 0, fmt.Errorf("failed to get chunkIDs from %s: %w", keys[0], err)
	}

	if !exists {
		return 0, nil
	}

	var deletedCount int
	for _, chunkID := range chunkIDs {
		chunkKey := "aws:chunk:" + chunkID
		var chunk meta.Chunk
		exists, err := txn.Get(chunkKey, &chunk)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get chunk %s: %v", chunkID, err)
			return 0, fmt.Errorf("failed to get chunk %s: %w", chunkKey, err)
		}

		if !exists {
			continue // 已经不存在，跳过
		}

		if chunk.RefCount > 1 {
			//logger.GetLogger("boulder").Debugf("chunk %s has ref more than one chunk", chunkID)
			chunk.RefCount--
			if err := txn.Set(chunkKey, &chunk); err != nil {
				logger.GetLogger("boulder").Errorf("failed to set chunk %s: %v", chunkID, err)
				return 0, fmt.Errorf("failed to update refCount for chunk %s: %w", chunkKey, err)
			}
		} else {
			if err := txn.Delete(chunkKey); err != nil {
				logger.GetLogger("boulder").Errorf("failed to delete chunk %s: %v", chunkID, err)
				return 0, fmt.Errorf("failed to delete chunk %s: %w", chunkKey, err)
			} else {
				//logger.GetLogger("boulder").Infof("deleted chunk %s", chunkID)
				deletedCount++
			}
		}
	}

	// 删除 GC 记录本身
	if err := txn.Delete(keys[0]); err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete chunk %s: %v", keys[0], err)
		return 0, fmt.Errorf("failed to delete task key %s: %w", keys[0], err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit txn: %v", err)
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return deletedCount, nil
}

func (g *GCService) cleanOne4Block(prefix string) (int, error) {
	txn, err := g.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer txn.Rollback()

	// 扫描一个前缀条目
	keys, _, err := txn.Scan(prefix, "", 1)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to scan keys: %v", err)
		return 0, fmt.Errorf("failed to scan: %w", err)
	}

	// 没有更多键了
	if len(keys) == 0 {
		//logger.GetLogger("boulder").Infof("no keys found, finish to task %s", prefix)
		return 0, nil
	} else {
		logger.GetLogger("boulder").Infof("found %s to clean ", keys[0])
	}

	var gcBlocks []*GCBlock
	exists, err := txn.Get(keys[0], &gcBlocks)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get blockIDs: %s  %v", keys[0], err)
		return 0, fmt.Errorf("failed to get blockIDs from %s: %w", keys[0], err)
	}

	if !exists {
		logger.GetLogger("boulder").Infof("not found %s to clean ", keys[0])
		return 0, nil
	}
	bs := block.GetBlockService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get block service")
		return 0, fmt.Errorf("failed to get block service")
	}

	var deletedCount int
	for _, gcBlock := range gcBlocks {
		blockKey := "aws:block:" + gcBlock.BlockID
		var _block meta.Block
		exists, err := txn.Get(blockKey, &_block)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get block %s: %v", blockKey, err)
			return 0, fmt.Errorf("failed to get block %s: %w", blockKey, err)
		}
		if !exists {
			// 索引元数据都不存在，就直接删除实际数据了
			err := bs.RemoveBlock(gcBlock.StorageID, gcBlock.BlockID)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to remove block %#v: %v", gcBlock, err)
				return 0, fmt.Errorf("failed to remove block %#v: %w", gcBlock, err)
			} else {
				logger.GetLogger("boulder").Infof("removed block %#v", gcBlock)
			}
			deletedCount++
			continue
		}
		var chunkIDs []string
		for _, chunk := range _block.ChunkList {
			_key := "aws:chunk:" + chunk.Hash
			chunkIDs = append(chunkIDs, _key)
		}
		chunks, err := txn.BatchGet(chunkIDs)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to batch get chunks: %v", err)
			return 0, fmt.Errorf("failed to batch get chunks: %w", err)
		}
		// 看看是否已经没有任何索引关联
		canDel := true
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
				_key := "aws:chunk:" + k
				err := txn.Delete(_key)
				if err != nil {
					logger.GetLogger("boulder").Errorf("failed to delete chunk %s: %v", k, err)
				} else {
					logger.GetLogger("boulder").Infof("deleted ref = 0 chunk %s", _key)
				}
			}
		}
		if canDel {
			// 先删除索引元数据
			err := txn.Delete(blockKey)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to delete block %s: %v", blockKey, err)
				return 0, fmt.Errorf("failed to delete chunk %s: %w", blockKey, err)
			}
			// 再删除实际数据
			err = bs.RemoveBlock(gcBlock.StorageID, gcBlock.BlockID)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to remove block %s: %v", gcBlock.BlockID, err)
				return 0, fmt.Errorf("failed to remove block %s: %w", gcBlock.BlockID, err)
			}

			deletedCount++
		}
	}
	// 删除 GC 记录本身
	if err := txn.Delete(keys[0]); err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete block %s: %v", keys[0], err)
		return 0, fmt.Errorf("failed to delete task key %s: %w", keys[0], err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit txn: %v", err)
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return deletedCount, nil
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
	chunk2block := make(map[string]string, 0)

	blockCh, errCh := s.Instance.List()
	for blockID := range blockCh {
		_block, err := bs.ReadBlock(s.ID, blockID)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to read block %s: %v", blockID, err)
			continue
		}
		logger.GetLogger("boulder").Infof("read block %s chunk %#v", blockID, len(_block.BlockHeader.ChunkList))
		for _, chunk := range _block.BlockHeader.ChunkList {
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
