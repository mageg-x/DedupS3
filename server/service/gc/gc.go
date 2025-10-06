/*
 * Copyright (C) 2025-2025 raochaoxun <raochaoxun@gmail.com>.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package gc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
	xcache "github.com/mageg-x/boulder/internal/storage/cache"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/internal/utils"
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

type GCItem struct {
	StorageID string `json:"StorageID" msgpack:"StorageID"`
	ID        string `json:"ID" msgpack:"ID"`
}

type GCData struct {
	CreateAt time.Time `json:"CreateAt" msgpack:"CreateAt"`
	Items    []GCItem  `json:"Items" msgpack:"Items"`
}

type GCBlock struct {
	GCData
}

type GCChunk struct {
	GCData
}

type GCDedup struct {
	GCData
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
		logger.GetLogger("boulder").Errorf("failed to get kv store for gc: %v", err)
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
	// chunks
	err := g.clean(GCChunkPrefix)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to clean up chunks: %v", err)
		return
	}

	//dedup
	err = g.clean(GCDedupPrefix)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to clean up dedup: %v", err)
		return
	}

	// 清理 blocks
	err = g.clean(GCBlockPrefix)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to clean up blocks: %v", err)
		return
	}

	logger.GetLogger("boulder").Tracef("garbage collection scan completed")
}

func (g *GCService) clean(prefix string) error {
	var lastErr error

	start := time.Now()
	logger.GetLogger("boulder").Tracef("garbage collection %s starting", prefix)
	defer func() {
		logger.GetLogger("boulder").Tracef("garbage collection %s completed in %s", prefix, time.Since(start))
	}()

	nextKey := ""
	for {
		// 单次扫描并处理一个 GC 条目
		switch prefix {
		case GCChunkPrefix:
			nextKey, _ = g.cleanOne4Chunk(prefix, nextKey)
		case GCBlockPrefix:
			nextKey, _ = g.cleanOne4Block(prefix, nextKey)
		case GCDedupPrefix:
			nextKey, _ = g.dedupOne4Block(prefix, nextKey)
		default:
			logger.GetLogger("boulder").Debugf("cleaning up chunk prefix %s", prefix)
		}

		if nextKey == "" {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
	return lastErr
}

// cleanOne 处理一个 GC 条目（一个 keys[0] 对应的 chunkIDs）
func (g *GCService) cleanOne4Chunk(prefix, startKey string) (nextKey string, err error) {
	nextKey = utils.NextKey(startKey)
	txn, err := g.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
		return nextKey, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer txn.Rollback()

	// 扫描一个前缀条目
	keys, nk, err := txn.Scan(prefix, startKey, 1)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to scan keys: %v", err)
		return nextKey, fmt.Errorf("failed to scan: %w", err)
	}

	// 没有更多键了
	if len(keys) == 0 {
		//logger.GetLogger("boulder").Infof("no keys found, finish to gc %s", prefix)
		return "", nil
	}
	nextKey = nk
	curKey := keys[0]

	var gcChunk GCChunk
	exists, err := txn.Get(curKey, &gcChunk)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get chunkids: %s  %v", curKey, err)
		return nextKey, fmt.Errorf("failed to get chunkIDs from %s: %w", curKey, err)
	}

	if !exists {
		logger.GetLogger("boulder").Errorf("gcchunk %s not found ", curKey)
		return nextKey, fmt.Errorf("gcchunk %s does not exist", curKey)
	}

	//hashfile, _ := os.OpenFile("clean.chunkid", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	//defer hashfile.Close()
	var StorageID string
	blockMap := make(map[string]bool)
	newItems := make([]GCItem, 0)
	for _, item := range gcChunk.Items {
		//hashfile.WriteString(fmt.Sprintf("%s\n", chunkID))
		chunkKey := meta.GenChunkKey(item.StorageID, item.ID)
		StorageID = item.StorageID
		if cache, e := xcache.GetCache(); e == nil && cache != nil {
			_ = cache.Del(context.Background(), chunkKey)
		}
		err = utils.RetryCall(3, func() error {
			_txn, err := g.kvstore.BeginTxn(context.Background(), nil)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer func() {
				if _txn != nil {
					_txn.Rollback()
				}
			}()
			var chunk meta.Chunk
			exists, err = _txn.Get(chunkKey, &chunk)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to get chunk %s: %v", item.ID, err)
				return fmt.Errorf("failed to get chunk %s: %w", chunkKey, err)
			}

			if !exists {
				logger.GetLogger("boulder").Infof("chunk %s not found ", item.ID)
				return nil // 已经不存在，跳过
			}

			if chunk.RefCount > 1 {
				//logger.GetLogger("boulder").Debugf("chunk %s has ref more than one chunk", chunkID)
				chunk.RefCount--
				if err := _txn.Set(chunkKey, &chunk); err != nil {
					logger.GetLogger("boulder").Errorf("failed to set chunk %s: %v", chunk.Hash, err)
					return fmt.Errorf("failed to update refCount for chunk %s: %w", chunkKey, err)
				} else {
					logger.GetLogger("boulder").Infof("updated refCount for chunk %s", chunk.Hash)
				}
			} else {
				if err = _txn.Delete(chunkKey); err != nil {
					logger.GetLogger("boulder").Errorf("failed to delete chunk %s: %v", chunk.Hash, err)
					return fmt.Errorf("failed to delete chunk %s: %w", chunk.Hash, err)
				} else {
					logger.GetLogger("boulder").Infof("deleted chunk %s", chunk.Hash)
					blockMap[chunk.BlockID] = true
				}
			}
			// 提交事务
			if err = _txn.Commit(); err != nil {
				logger.GetLogger("boulder").Errorf("failed to commit txn: %v", err)
				return fmt.Errorf("failed to commit transaction: %w", err)
			}
			_txn = nil
			return nil
		})
		if err != nil {
			newItems = append(newItems, item)
		}
	}

	// 删除 GC 记录本身
	err = utils.RetryCall(3, func() error {
		_txn, err := g.kvstore.BeginTxn(context.Background(), nil)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		defer func() {
			if _txn != nil {
				_txn.Rollback()
			}
		}()

		if len(newItems) > 0 {
			// 有些处理失败的留着下次重试
			gcChunk.Items = newItems
			gcChunk.CreateAt = time.Now().UTC()
			if err := _txn.Set(curKey, &gcChunk); err != nil {
				logger.GetLogger("boulder").Errorf("failed to set chunk %s: %v", curKey, err)
				return fmt.Errorf("failed to set chunk %s: %w", curKey, err)
			}
		} else {
			if err = _txn.Delete(curKey); err != nil {
				logger.GetLogger("boulder").Errorf("failed to delete chunk %s: %v", curKey, err)
				return fmt.Errorf("failed to delete gc key %s: %w", curKey, err)
			}
		}

		// 相关 block 也要 再次检查
		if len(blockMap) > 0 {
			gcKey := GCDedupPrefix + utils.GenUUID()
			gcData := GCDedup{
				GCData: GCData{
					CreateAt: time.Now().UTC(),
					Items:    make([]GCItem, 0),
				},
			}
			for blockID := range blockMap {
				gcData.Items = append(gcData.Items, GCItem{StorageID: StorageID, ID: blockID})
			}
			err = _txn.Set(gcKey, gcData)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to set gc blocks gcKey %s error : %v", gcKey, err)
				return fmt.Errorf("failed to set gc blocks gcKey %s: %w", gcKey, err)
			}
		}
		// 提交事务
		if err = _txn.Commit(); err != nil {
			logger.GetLogger("boulder").Errorf("failed to commit txn: %v", err)
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		_txn = nil
		return nil
	})

	return nextKey, err
}

func (g *GCService) cleanOne4Block(prefix, startKey string) (nextKey string, err error) {
	nextKey = utils.NextKey(startKey)
	txn, err := g.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
		return nextKey, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer txn.Rollback()

	// 扫描一个前缀条目
	keys, nk, err := txn.Scan(prefix, startKey, 1)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to scan keys: %v", err)
		return nextKey, fmt.Errorf("failed to scan: %w", err)
	}

	// 没有更多键了
	if len(keys) == 0 {
		//logger.GetLogger("boulder").Infof("no keys found, finish to gc %s", prefix)
		return "", nil
	}
	nextKey = nk
	curKey := keys[0]

	var gcBlock GCBlock
	exists, err := txn.Get(curKey, &gcBlock)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get gcblockIDs: %s  %v", curKey, err)
		return nextKey, fmt.Errorf("failed to get gcblockIDs from %s: %w", curKey, err)
	}

	if !exists {
		logger.GetLogger("boulder").Infof("gcblock %s does not exist", curKey)
		return nextKey, fmt.Errorf("gcblock %s does not exist", curKey)
	}

	// block 清理要延迟，避免有些关联的 meta 数据还没有提交，因为block 是data 先提交， meta 后提交
	if time.Since(gcBlock.CreateAt) < 5*time.Minute {
		// 没有超过 3 分钟
		return nextKey, fmt.Errorf("gcblock %s is not old enought", curKey)
	}

	bs := block.GetBlockService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get block service")
		return nextKey, errors.New("failed to get block service")
	}

	for _, item := range gcBlock.Items {
		blockKey := meta.GenBlockKey(item.StorageID, item.ID)
		if cache, e := xcache.GetCache(); e == nil && cache != nil {
			_ = cache.Del(context.Background(), blockKey)
		}
		var _block meta.Block
		exists, err = txn.Get(blockKey, &_block)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get gcblock %s: %v", blockKey, err)
			return nextKey, fmt.Errorf("failed to get gcblock %s: %w", blockKey, err)
		}

		if !exists {
			// 索引元数据都不存在，直接删除
			err = utils.RetryCall(3, func() error {
				return bs.RemoveBlock(item.StorageID, item.ID)
			})
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to remove gcblock %#v: %v", item, err)
				return nextKey, fmt.Errorf("failed to remove gcblock %s: %w", item.ID, err)
			}
			continue
		} else {
			var chunkIDs []string
			for _, chunk := range _block.ChunkList {
				if chunk.Hash != meta.NONE_CHUNK_ID {
					_key := meta.GenChunkKey(item.StorageID, chunk.Hash)
					chunkIDs = append(chunkIDs, _key)
				}
			}

			// 看看是否已经没有任何索引关联
			canDel := true
			if len(chunkIDs) > 0 {
				chunks, err := txn.BatchGet(chunkIDs)
				if err != nil {
					logger.GetLogger("boulder").Errorf("failed to batch get gcchunks: %v", err)
					return nextKey, fmt.Errorf("failed to batch get gcchunks: %w", err)
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
						logger.GetLogger("boulder").Debugf("cannot delete block %s chunk %#v", _chunk.BlockID, _chunk)
						canDel = false
						break
					} else {
						// 顺手清理掉 引用为 0 的元数据
						_key := meta.GenChunkKey(item.StorageID, k)
						err := g.kvstore.Delete(_key)
						if err != nil {
							logger.GetLogger("boulder").Errorf("failed to delete chunk %s: %v", k, err)
						}
					}
				}
			}

			if canDel {
				err = utils.RetryCall(3, func() error {
					// 先删除block索引元数据
					err := g.kvstore.Delete(blockKey)
					if err != nil {
						logger.GetLogger("boulder").Errorf("failed to delete block %s: %v", blockKey, err)
						return fmt.Errorf("failed to delete chunk %s: %w", blockKey, err)
					}
					// 再删除实际数据
					err = bs.RemoveBlock(item.StorageID, item.ID)
					if err != nil {
						logger.GetLogger("boulder").Errorf("failed to remove block %s: %v", item.ID, err)
						return fmt.Errorf("failed to remove block %s: %w", item.ID, err)
					}
					return nil
				})
				if err != nil {
					return nextKey, fmt.Errorf("failed to delete block %s: %w", blockKey, err)
				}
			}
		}
	}

	err = utils.RetryCall(3, func() error {
		// 删除 GC 记录本身
		err := g.kvstore.Delete(curKey)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to delete block %s: %v", curKey, err)
			return fmt.Errorf("failed to delete gc key %s: %w", curKey, err)
		}
		return err
	})

	return nextKey, err
}

func (g *GCService) dedupOne4Block(prefix, startKey string) (nextKey string, err error) {
	nextKey = utils.NextKey(startKey)

	bs := block.GetBlockService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get block service")
		return nextKey, errors.New("failed to get block service")
	}

	txn, err := g.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
		return nextKey, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer txn.Rollback()

	// 扫描一个前缀条目
	keys, nk, err := txn.Scan(prefix, startKey, 1)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to scan keys: %v", err)
		return nextKey, fmt.Errorf("failed to scan: %w", err)
	}

	// 没有更多键了
	if len(keys) == 0 {
		//logger.GetLogger("boulder").Infof("no keys found, finish to gc %s", prefix)
		return nextKey, nil
	}

	nextKey = nk
	curKey := keys[0]

	var gcDedup GCDedup
	exists, err := txn.Get(keys[0], &gcDedup)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get gcDedup: %s  %v", curKey, err)
		return nextKey, fmt.Errorf("failed to get gcDedup from %s: %w", curKey, err)
	}

	if !exists {
		logger.GetLogger("boulder").Infof("gcDedup %s not found ", curKey)
		return nextKey, fmt.Errorf("gcDedup %s does not exist", curKey)
	}

	if time.Since(gcDedup.CreateAt) < 5*time.Minute {
		return nextKey, fmt.Errorf("gcDedup %s is not old enought", curKey)
	}

	cfg := config.Get()
	newItems := make([]GCItem, 0)
	for _, item := range gcDedup.Items {
		blockKey := meta.GenBlockKey(item.StorageID, item.ID)
		_block := meta.Block{}

		exists, err = txn.Get(blockKey, &_block)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get block %s: %v", blockKey, err)
			return nextKey, fmt.Errorf("failed to get block %s: %w", blockKey, err)
		}
		if !exists {
			logger.GetLogger("boulder").Errorf("block %s not found ", blockKey)
		}

		if _block.Finally || time.Since(_block.UpdatedAt) > cfg.Block.MaxRetentionTime {
			if err := g.shrinkBlock(&_block); err != nil {
				newItems = append(newItems, item)
			}
		} else {
			newItems = append(newItems, item)
		}
	}

	if len(newItems) > 0 && len(newItems) == len(gcDedup.Items) {
		// 没有处理任何数据
		return nextKey, fmt.Errorf("gcDedup %s proccess no data", nextKey)
	}

	err = utils.RetryCall(3, func() error {
		_txn, err := g.kvstore.BeginTxn(context.Background(), nil)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		if len(newItems) > 0 {
			// 重新写回未处理的 item，留着下次再处理
			gcDedup.Items = newItems
			gcDedup.CreateAt = time.Now().UTC()
			if err := _txn.Set(curKey, &gcDedup); err != nil {
				logger.GetLogger("boulder").Errorf("failed to reset gcDedup %s: %v", curKey, err)
				return fmt.Errorf("failed to reset gcDedup %s: %w", curKey, err)
			}
		} else {
			// 删除 GC 记录本身
			if err := _txn.Delete(curKey); err != nil {
				logger.GetLogger("boulder").Errorf("failed to delete block %#v: %v", gcDedup, err)
				return fmt.Errorf("failed to delete block %#v: %w", gcDedup, err)
			}
		}

		// 提交事务
		if err := _txn.Commit(); err != nil {
			logger.GetLogger("boulder").Errorf("failed to commit transaction for block  %#v: %v", gcDedup, err)
			return fmt.Errorf("failed to commit transaction for block %#v: %w", gcDedup, err)
		}
		_txn = nil
		return nil
	})

	return nextKey, err
}

func (g *GCService) shrinkBlock(_block *meta.Block) error {
	chunks := make(map[string]meta.BlockChunk, 0)

	for _, _ck := range _block.ChunkList {
		if _ck.Hash != meta.NONE_CHUNK_ID {
			chunks[_ck.Hash] = _ck
		}
	}
	if len(chunks) == len(_block.ChunkList) {
		// 全是有效chunks，无空洞chunk
		return nil
	}

	blockKey := meta.GenBlockKey(_block.StorageID, _block.ID)
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		_ = cache.Del(context.Background(), blockKey)
	}
	bs := block.GetBlockService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get block service")
		return fmt.Errorf("failed to get block service")
	}

	if len(chunks) == 0 {
		// 全是空洞chunk 可以直接删除
		err := utils.RetryCall(5, func() error {
			err := bs.RemoveBlock(_block.StorageID, _block.ID)
			if err == nil {
				return g.kvstore.Delete(blockKey)
			}
			return err
		})
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to set block %#v: %v", _block, err)
			return fmt.Errorf("failed to delete block %s : %w", _block.ID, err)
		}
		return nil
	}

	// 存在空洞 chunks，开始去掉空洞chunk
	blockData, err := bs.ReadBlock(_block.StorageID, _block.ID)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to read block %#v data: %v", _block, err)
		return fmt.Errorf("failed to read block %s data: %w", _block.ID, err)
	}

	chunkMap := make(map[string]bool, 0)
	for _, _ck := range _block.ChunkList {
		chunkMap[_ck.Hash] = true
	}

	newBlockData := &meta.BlockData{
		BlockHeader: blockData.BlockHeader,
		Data:        make([]byte, 0, _block.TotalSize),
	}
	newBlockData.ChunkList = make([]meta.BlockChunk, 0)
	newBlockData.TotalSize = 0
	offset := int32(0)
	for _, _ck := range blockData.ChunkList {
		if _, ok := chunkMap[_ck.Hash]; ok {
			newBlockData.ChunkList = append(newBlockData.ChunkList, _ck)
			newBlockData.Data = append(newBlockData.Data, blockData.Data[offset:offset+_ck.Size]...)
			newBlockData.TotalSize += int64(_ck.Size)
		}
		offset += _ck.Size
	}

	newBlockData.UpdatedAt = time.Now().UTC()
	newBlockData.CalcChunkHash()
	_block.BlockHeader = newBlockData.BlockHeader
	copy(_block.ChunkList, newBlockData.ChunkList)

	err = bs.WriteBlock(context.Background(), _block.StorageID, newBlockData)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to write block data: %v", err)
		return fmt.Errorf("failed to write block data: %w", err)
	}

	err = utils.RetryCall(5, func() error {
		return g.kvstore.Set(blockKey, _block)
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to set block %#v: %v", _block, err)
		return fmt.Errorf("failed to set block %s : %w", _block.ID, err)
	}
	return nil
}
