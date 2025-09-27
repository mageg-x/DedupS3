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
package block

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/twmb/murmur3"
	"github.com/vmihailenco/msgpack/v5"

	xconf "github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
	sb "github.com/mageg-x/boulder/internal/storage/block"
	xcache "github.com/mageg-x/boulder/internal/storage/cache"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
	"github.com/mageg-x/boulder/service/storage"
)

var (
	instance *BlockService
	mu       = sync.Mutex{}
)

type BlockService struct {
	kvstore      kv.KVStore
	preBlocks    []*meta.Block
	lockers      []sync.Mutex // 为每个块单独设置锁
	cancelMap    map[string]context.CancelFunc
	cancelLocker sync.Mutex
}

func GetBlockService() *BlockService {
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
	cfg := xconf.Get()
	instance = &BlockService{
		kvstore:      store,
		preBlocks:    make([]*meta.Block, cfg.Block.ShardNum),
		lockers:      make([]sync.Mutex, cfg.Block.ShardNum),
		cancelMap:    make(map[string]context.CancelFunc),
		cancelLocker: sync.Mutex{},
	}
	//instance.doSyncBlock(context.Background())
	return instance
}

func (s *BlockService) doSyncBlock(ctx context.Context) {
	go func() {
		cfg := xconf.Get()

		for {
			select {
			case <-ctx.Done(): // 可以响应取消信号
				logger.GetLogger("boulder").Info("sync block stopping due to context cancellation")
				return
			default:
				// 正常的处理逻辑
			}

			// 处理滞留在内存中的block
			for i := 0; i < cfg.Block.ShardNum; i++ {
				utils.WithLock(&s.lockers[i], func() error {
					flushBlock := s.preBlocks[i]
					// 一小时还没有 提交的快，当成终结块提交吧
					if flushBlock != nil && !flushBlock.Finally && time.Since(flushBlock.UpdatedAt) > cfg.Block.MaxRetentionTime {
						oldVer := flushBlock.Ver
						flushBlock.Finally = true
						flushBlock.Ver = meta.BLOCK_FINALY_VER
						err := s.doFlushBlock(context.Background(), flushBlock)
						if err != nil {
							// 恢复
							flushBlock.Ver = oldVer
							flushBlock.Finally = false
							logger.GetLogger("boulder").Warnf("failed to flush block %s: %v", flushBlock.ID, err)
							return fmt.Errorf("failed to flush block %s: %w", flushBlock.ID, err)
						} else {
							s.preBlocks[i] = nil
							return nil
						}
					}
					return nil
				})
			}

			blockPath := filepath.Join(cfg.Node.LocalDir, "block")
			if err := os.MkdirAll(blockPath, 0755); err != nil {
				logger.GetLogger("boulder").Errorf("failed to create disk store directory: %v", err)
				continue
			}

			// 递归 列出 blockPath下所有文件，并且按照字母顺序排序
			blockFiles, err := utils.ReadFilesRecursive(blockPath)
			if err != nil {
				logger.GetLogger("boulder").Debugf("failed to read block files: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			var wg sync.WaitGroup
			sem := make(chan struct{}, cfg.Block.UploadParallelNum) // 信号量，控制并发
			for _, blockFile := range blockFiles {
				select {
				case <-ctx.Done(): // 可以响应取消信号
					logger.GetLogger("boulder").Info("sync block stopping due to context cancellation")
					return
				default:
					// 正常的处理逻辑
				}

				// 清理 长时间未处理的 tmpfile
				tmpfile := filepath.Join(blockPath, blockFile)
				// 获取文件信息
				fileInfo, err := os.Stat(tmpfile)
				if err != nil {
					logger.GetLogger("boulder").Debugf("failed to stat tmp file: %v", err)
					continue
				}

				if strings.HasSuffix(tmpfile, ".tmp") {
					// 判断修改时间是否早于 24 小时前
					if time.Since(fileInfo.ModTime()) > 24*time.Hour {
						// 超时了，可以安全删除
						if err := os.Remove(tmpfile); err != nil {
							logger.GetLogger("boulder").Warnf("remove stale tmp file %s failed: %v", tmpfile, err)
						} else {
							logger.GetLogger("boulder").Infof("cleaned up stale tmp file (timeout): %s", tmpfile)
						}
					}
					continue
				}

				// 跳过 非 finaly 版本
				ver, err := utils.ReadBlockVerFromFile(tmpfile)
				if err != nil {
					logger.GetLogger("boulder").Errorf("failed to read block version: %v", err)
					continue
				}
				if ver != meta.BLOCK_FINALY_VER && time.Since(fileInfo.ModTime()) < 30*time.Second {
					// 延迟30秒上传，可能存在 覆写，节省重复上传流量
					continue
				}

				// 在获取信号量之前检查上下文取消
				select {
				case sem <- struct{}{}:
					wg.Add(1) // 增加计数
				case <-ctx.Done():
					logger.GetLogger("boulder").Info("sync block stopping due to context cancellation")
					return
				}
				go func(upfile string, filever int32) {
					defer func() {
						<-sem     // 释放令牌
						wg.Done() // 减少计数
					}()

					data, err := os.ReadFile(upfile)
					if err != nil {
						logger.GetLogger("boulder").Infof("read file %s failed %v", upfile, err)
						return
					}

					if len(data) < 4 {
						logger.GetLogger("boulder").Errorf("file %s is too short", upfile)
						os.Remove(upfile)
						return
					}
					data = data[4:]
					var _block meta.BlockData
					if err := msgpack.Unmarshal(data, &_block); err != nil {
						os.Remove(upfile)
						return
					}

					_ctx, cancel := context.WithCancel(ctx)
					defer cancel() // 确保无论如何都会取消
					utils.WithLock(&s.cancelLocker, func() error {
						oldCancel := s.cancelMap[_block.ID]
						if oldCancel != nil {
							oldCancel()
						}
						s.cancelMap[_block.ID] = cancel
						return nil
					})

					ss := storage.GetStorageService()
					if ss == nil {
						logger.GetLogger("boulder").Errorf("get nil storage service")
						return
					}

					st, err := ss.GetStorage(_block.StorageID)
					if err != nil {
						logger.GetLogger("boulder").Errorf("get storage %#v failed: %v", _block, err)
						return
					}

					defer func() {
						utils.WithLock(&s.cancelLocker, func() error {
							delete(s.cancelMap, _block.ID)
							return nil
						})
					}()

					if err := st.Instance.WriteBlock(_ctx, _block.ID, data, _block.Ver); err == nil {
						curver, err := utils.ReadBlockVerFromFile(upfile)
						if err == nil && filever != curver {
							// 文件已经更新 ，不能删除
						} else {
							os.Remove(upfile)
						}
					} else {
						logger.GetLogger("boulder").Errorf("write block %s failed: %v", _block.ID, err)
					}
				}(tmpfile, ver)
			}
			wg.Wait() // 等待本批次所有上传完成

			utils.CleanEmptyDirsRecursive(blockPath)

			time.Sleep(1 * time.Second)
		}
	}()
}

func (s *BlockService) PutChunk(chunk *meta.Chunk, obj *meta.BaseObject) (*meta.Block, error) {
	cfg := xconf.Get()
	// 分散目的是为了提高并发性， 同一个文件，多个multi part upload， 可以并发写
	h := murmur3.Sum32([]byte(obj.Bucket + obj.Key))
	i := h % uint32(cfg.Block.ShardNum)

	var flushBlock *meta.Block
	var clone *meta.Block

	err := utils.WithLock(&s.lockers[i], func() error {
		if chunk != nil {
			if chunk.Size != int32(len(chunk.Data)) {
				logger.GetLogger("boulder").Errorf("chunk %s/%s/%s size %d:%d not match", obj.Bucket, obj.Key, chunk.Hash, chunk.Size, len(chunk.Data))
				return fmt.Errorf("chunk %s/%s/%s size %d:%d not match", obj.Bucket, obj.Key, chunk.Hash, chunk.Size, len(chunk.Data))
			}

			if chunk.Data == nil {
				logger.GetLogger("boulder").Errorf("chunk data is nil: %#v", chunk)
				return fmt.Errorf("chunk %s data is nil ", chunk.Hash)
			}

			curBlock := s.preBlocks[i]
			if curBlock == nil {
				curBlock = meta.NewBlock(obj.DataLocation)
				s.preBlocks[i] = curBlock
			}

			exists := false
			chunk.BlockID = curBlock.ID
			for _, _ck := range curBlock.ChunkList {
				if _ck.Hash == chunk.Hash {
					exists = true
					break
				}
			}

			if !exists {
				curBlock.ChunkList = append(curBlock.ChunkList, meta.BlockChunk{Hash: chunk.Hash, Size: chunk.Size, Data: chunk.Data})
				chunk.Data = nil
				curBlock.TotalSize += int64(chunk.Size)
				curBlock.UpdatedAt = time.Now().UTC()
				// 返回 chunk所属的block
				clone = curBlock.Clone(false)
			}

			if curBlock.TotalSize > int64(cfg.Block.MaxSize) {
				// 块超过大小，保存到存储
				flushBlock = s.preBlocks[i]
				flushBlock.Finally = true
			}
		} else {
			// 对象结束时候会发一个 nil chunk 表示 对象结束了，需要保存 blcok
			flushBlock = s.preBlocks[i]
			// 一小时还没有更新和提交的快，当成终结块提交吧
			if flushBlock != nil && time.Since(flushBlock.UpdatedAt) > cfg.Block.MaxRetentionTime {
				logger.GetLogger("boulder").Errorf("pass time %v:%v", time.Since(flushBlock.UpdatedAt), cfg.Block.MaxRetentionTime)
				flushBlock.Finally = true
			}
		}

		if flushBlock != nil {
			oldVer := flushBlock.Ver
			if flushBlock.Finally {
				flushBlock.Ver = meta.BLOCK_FINALY_VER
			} else {
				flushBlock.Ver += 1
			}

			logger.GetLogger("boulder").Infof("ready to flush one block %s,  %d chunks", flushBlock.ID, len(flushBlock.ChunkList))
			err := s.doFlushBlock(context.Background(), flushBlock)
			if err != nil {
				// 恢复
				flushBlock.Ver = oldVer + 1
				flushBlock.Finally = false

				logger.GetLogger("boulder").Warnf("failed to flush block %s: %v", flushBlock.ID, err)
				return fmt.Errorf("failed to flush block %s: %w", flushBlock.ID, err)
			} else if flushBlock.Finally {
				// 成功的话， 把 flushBlock 摘出来
				s.preBlocks[i] = nil
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	} else {
		return clone, nil
	}
}

// doFlushBlock 本函数提供同步写数据能力
func (s *BlockService) doFlushBlock(ctx context.Context, block *meta.Block) error {
	cfg := xconf.Get()
	blockData := meta.BlockData{
		BlockHeader: meta.BlockHeader{
			ID:        block.ID,
			TotalSize: block.TotalSize,
			Ver:       block.Ver,
			Finally:   block.Finally,
			Location:  cfg.Node.LocalNode,
			ChunkList: make([]meta.BlockChunk, 0, len(block.ChunkList)),
			StorageID: block.StorageID,
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		},

		Data: make([]byte, 0, cfg.Block.MaxSize),
	}

	// 重新检查 size
	size1, size2 := int32(0), int32(0)
	for i := 0; i < len(block.ChunkList); i++ {
		size1 += block.ChunkList[i].Size
		size2 += int32(len(block.ChunkList[i].Data))
		if block.ChunkList[i].Size != int32(len(block.ChunkList[i].Data)) {
			logger.GetLogger("boulder").Errorf("chunk %s size not match %d:%d", block.ChunkList[i].Hash, block.ChunkList[i].Size, len(block.ChunkList[i].Data))
			return fmt.Errorf("chunk %s size not match", block.ChunkList[i].Hash)
		}
		_chunk := meta.BlockChunk{
			Hash: block.ChunkList[i].Hash,
			Size: block.ChunkList[i].Size,
		}
		blockData.ChunkList = append(blockData.ChunkList, _chunk)
		blockData.Data = append(blockData.Data, block.ChunkList[i].Data...)
	}

	blockData.RealSize = block.TotalSize
	block.RealSize = block.TotalSize
	if size1 != size2 || blockData.TotalSize != blockData.RealSize {
		logger.GetLogger("boulder").Errorf("flush block %s size mot match %d:%d:%d:%d", block.ID, size1, size2, blockData.RealSize, blockData.RealSize)
		return fmt.Errorf("flush block %s size mot match %d:%d:%d:%d", block.ID, size1, size2, blockData.RealSize, blockData.RealSize)
	}
	logger.GetLogger("boulder").Infof("flush block data %s total size %d real size %d data size %d",
		blockData.ID, blockData.TotalSize, blockData.RealSize, len(blockData.Data))

	// 计算md5，数据校验
	blockData.CalcChunkHash()
	block.Etag = blockData.Etag

	err := s.WriteBlock(ctx, block.StorageID, &blockData)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// 被取消
			return nil
		}
		logger.GetLogger("boulder").Warnf("failed to WriteBlock %s: %v", block.ID, err)
		return fmt.Errorf("failed to WriteBlock %s: %w", block.ID, err)
	}
	block.Compressed = blockData.Compressed
	block.Encrypted = blockData.Encrypted
	block.RealSize = blockData.RealSize

	return nil
}

func (s *BlockService) WriteBlock(ctx context.Context, storageID string, blockData *meta.BlockData) error {
	// 压缩Data
	if len(blockData.Data) > 1024 && utils.IsCompressible(blockData.Data, 4*1024, 0.9) {
		compress, err := utils.Compress(blockData.Data)
		if err == nil && compress != nil && float64(len(compress))/float64(len(blockData.Data)) < 0.9 {
			blockData.Data = compress
			blockData.Compressed = true
			blockData.RealSize = int64(len(compress))
		}
	}

	// 加密Data
	if len(blockData.Data) > 0 {
		encrypt, err := utils.Encrypt(blockData.Data, blockData.ID)
		if err == nil && encrypt != nil {
			blockData.Data = encrypt
			blockData.Encrypted = true
			blockData.RealSize = int64(len(encrypt))
		}
	}

	logger.GetLogger("boulder").Infof("flush block data size %d:%d, compress rate %.2f%%",
		blockData.TotalSize, blockData.RealSize, float64(100.0*blockData.RealSize)/float64(blockData.TotalSize))

	data, err := msgpack.Marshal(&blockData)
	if err != nil {
		logger.GetLogger("boulder").Debugf("msgpack marshal %s failed: %v", blockData.ID, err)
		return fmt.Errorf("msgpack marshal %s failed: %w", blockData.ID, err)
	}

	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("boulder").Errorf("get nil storage service")
		return fmt.Errorf("get nil storage service")
	}

	st, err := ss.GetStorage(storageID)
	if err != nil || st == nil || st.Instance == nil {
		logger.GetLogger("boulder").Errorf("get nil storage instance")
		return fmt.Errorf("get nil storage instance: %w", err)
	}

	err = st.Instance.WriteBlock(ctx, blockData.ID, data, blockData.Ver)
	if err != nil {
		logger.GetLogger("boulder").Debugf("write block %s failed: %v", blockData.ID, err)
		return fmt.Errorf("write block %s failed: %w", blockData.ID, err)
	} else {
		logger.GetLogger("boulder").Infof("finish write block %s success", blockData.ID)
	}

	return nil
}

func (s *BlockService) ReadBlock(storageID, blockID string) (*meta.BlockData, error) {
	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("boulder").Errorf("get nil storage service")
		return nil, fmt.Errorf("get nil storage service")
	}

	st, err := ss.GetStorage(storageID)
	if err != nil || st == nil || st.Instance == nil {
		logger.GetLogger("boulder").Errorf("get nil storage instance")
		return nil, fmt.Errorf("get nil storage instance: %w", err)
	}

	// 读取 block meta信息
	blockKey := meta.GenBlockKey(storageID, blockID)
	var blockMeta meta.Block
	exists, err := s.kvstore.Get(blockKey, &blockMeta)
	if err != nil && !exists {
		logger.GetLogger("boulder").Debugf("read block meta %s failed: %v", blockID, err)
		return nil, fmt.Errorf("read block meta %s failed: %w", blockID, err)
	}

	// 读取block 数据
	data, err := st.Instance.ReadBlock(blockMeta.Location, blockID, 0, 0)
	if err != nil || len(data) == 0 {
		logger.GetLogger("boulder").Errorf("read block %s failed: %v", blockID, err)
		return nil, fmt.Errorf("read block %s failed: %w", blockID, err)
	}
	blockData := meta.BlockData{}
	err = msgpack.Unmarshal(data, &blockData)
	if err != nil {
		logger.GetLogger("boulder").Errorf("msgpack unmarshal block %s  data %d to struct failed: %v", blockID, len(data), err)
		return nil, fmt.Errorf("msgpack unmarshal block %s  data %d to struct failed: %w", blockID, len(data), err)
	}
	if blockID != blockData.ID {
		logger.GetLogger("boulder").Errorf("read block %s id not match block %s ", blockID, blockData.ID)
		return nil, fmt.Errorf("read block %s id not match block %s ", blockID, blockData.ID)
	}
	if blockData.Encrypted {
		_d, err := utils.Decrypt(blockData.Data, blockID)
		if err != nil {
			logger.GetLogger("boulder").Errorf("decrypt block %s failed: %v", blockID, err)
			return nil, fmt.Errorf("decrypt block %s failed: %w", blockID, err)
		}
		blockData.Data = _d
	}

	if blockData.Compressed {
		_d, err := utils.Decompress(blockData.Data)
		if err != nil {
			logger.GetLogger("boulder").Errorf("decompress block %s data failed: %v", blockID, err)
			return nil, fmt.Errorf("decompress block %s data failed: %w", blockID, err)
		}
		blockData.Data = _d
	}
	if blockData.TotalSize != int64(len(blockData.Data)) {
		logger.GetLogger("boulder").Errorf("read block %s size not match %d:%d ", blockID, blockData.TotalSize, len(blockData.Data))
		return nil, fmt.Errorf("block %s  data be damaged size not match %d:%d", blockID, blockData.TotalSize, len(blockData.Data))
	}
	return &blockData, nil
}

func (s *BlockService) ReadBlockHead(storageID, blockID string) (*meta.BlockHeader, error) {
	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("boulder").Errorf("get nil storage service")
		return nil, fmt.Errorf("get nil storage service")
	}

	st, err := ss.GetStorage(storageID)
	if err != nil || st == nil || st.Instance == nil {
		logger.GetLogger("boulder").Errorf("get nil storage instance")
		return nil, fmt.Errorf("get nil storage instance: %w", err)
	}
	cfg := xconf.Get()

	// 读取 block meta信息
	blockKey := meta.GenBlockKey(storageID, blockID)
	var blockMeta meta.Block
	exists, err := s.kvstore.Get(blockKey, &blockMeta)
	if err != nil && !exists {
		logger.GetLogger("boulder").Debugf("read block meta %s failed: %v", blockID, err)
		return nil, fmt.Errorf("read block meta %s failed: %w", blockID, err)
	}

	data, err := st.Instance.ReadBlock(blockMeta.Location, blockID, 0, int64(cfg.Block.MaxHeadSize))
	if err != nil {
		logger.GetLogger("boulder").Errorf("read block header %s failed: %v", blockID, err)
		return nil, fmt.Errorf("read block header %s failed: %w", blockID, err)
	}

	dec := msgpack.NewDecoder(bytes.NewReader(data))
	var header meta.BlockHeader
	err = dec.Decode(&header)
	if err != nil {
		logger.GetLogger("boulder").Errorf("decode block header %s failed: %v", blockID, err)
		return nil, fmt.Errorf("decode block header %s failed: %w", blockID, err)
	}

	return &header, nil
}

func (s *BlockService) RemoveBlock(storageID, blockID string) error {
	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("boulder").Errorf("get nil storage service")
		return fmt.Errorf("get nil storage service")
	}

	st, err := ss.GetStorage(storageID)
	if err != nil || st == nil || st.Instance == nil {
		logger.GetLogger("boulder").Errorf("get nil storage instance")
		return fmt.Errorf("get nil storage instance: %w", err)
	}

	err = st.Instance.DeleteBlock(blockID)
	if err != nil {
		if errors.Is(err, sb.ErrBlockNotFound) {
			return nil
		}
		logger.GetLogger("boulder").Debugf("failed to remove block %s: %v", blockID, err)
		return fmt.Errorf("failed to remove block %s: %w", blockID, err)
	}
	return nil
}

func (s *BlockService) BatchGet(storageID string, blockIds []string) ([]*meta.Block, error) {
	blockMap := make(map[string]*meta.Block)
	keys := make([]string, 0, len(blockIds))
	for _, id := range blockIds {
		key := meta.GenBlockKey(storageID, id)
		keys = append(keys, key)
	}
	batchSize := 100
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		batchKeys := keys[i:end]
		if cache, err := xcache.GetCache(); err == nil && cache != nil {
			result, err := cache.MGet(context.Background(), batchKeys)
			if err == nil {
				for k, item := range result {
					block := item.(*meta.Block)
					if block != nil {
						blockMap[block.ID] = block
					} else {
						cache.Del(context.Background(), k)
					}
				}
			}
		}

		newBatch := make([]string, 0, len(batchKeys))
		for _, key := range batchKeys {
			blockID := key[len("aws:block:"+storageID+":"):]
			_, ok := blockMap[blockID]
			if !ok {
				newBatch = append(newBatch, key)
			}
		}
		batchKeys = newBatch

		result, err := s.kvstore.BatchGet(batchKeys)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to batchGet blocks: %v", err)
			return nil, fmt.Errorf("failed to batchGet blocks: %w", err)
		}
		for k, v := range result {
			var block meta.Block
			err := json.Unmarshal(v, &block)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to Unmarshal block %s err: %v", k, err)
				return nil, fmt.Errorf("failed to Unmarshal block %s err: %w", k, err)
			}
			blockMap[block.ID] = &block

			if cache, err := xcache.GetCache(); err == nil && cache != nil {
				blockKey := meta.GenBlockKey(storageID, block.ID)
				err := cache.Set(context.Background(), blockKey, &block, time.Second*600)
				if err != nil {
					logger.GetLogger("boulder").Errorf("set block %s to cache failed: %v", blockKey, err)
				}
			}
		}
	}

	blocks := make([]*meta.Block, 0, len(blockIds))
	for _, blockID := range blockIds {
		block, ok := blockMap[blockID]
		if !ok {
			logger.GetLogger("boulder").Errorf("block %s not exist", blockID)
			return nil, fmt.Errorf("block %s not exist", blockID)
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}
