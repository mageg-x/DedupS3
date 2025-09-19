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
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	xconf "github.com/mageg-x/boulder/internal/config"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/murmur3"
	"github.com/vmihailenco/msgpack/v5"

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
	kvstore kv.KVStore

	preBlocks []*meta.Block
	lockers   []sync.Mutex // 为每个块单独设置锁

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
	instance.doSyncWorker()
	return instance
}

func readVersionFromFile(filename string) (int32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return -1, err
	}
	defer file.Close()

	var buf [4]byte
	n, err := file.Read(buf[:])
	if err != nil && err != io.EOF {
		return -1, err
	}
	if n < 4 {
		return -1, fmt.Errorf("文件太小，不足4字节")
	}

	bufferVer := int32(binary.BigEndian.Uint32(buf[:]))
	return bufferVer, nil
}

func (s *BlockService) doSyncWorker() {
	go func() {
		cfg := xconf.Get()
		for {
			files, _ := os.ReadDir(cfg.Block.UploadBufDir)
			var wg sync.WaitGroup
			sem := make(chan struct{}, cfg.Block.UploadParallelNum) // 信号量，控制并发
			for _, f := range files {
				if f.IsDir() {
					continue
				}
				// 获取文件信息（包含修改时间）
				info, err := f.Info()
				if err != nil {
					continue
				}

				if strings.HasSuffix(f.Name(), ".tmp") {
					// 清理 长时间未处理的 tmpfile
					tmpfile := fmt.Sprintf("%s/%s", cfg.Block.UploadBufDir, f.Name())

					// 判断修改时间是否早于 1 小时前
					if time.Since(info.ModTime()) > 1*time.Hour {
						// 超时了，可以安全删除
						if err := os.Remove(tmpfile); err != nil {
							logger.GetLogger("boulder").Warnf("remove stale tmp file %s failed: %v", tmpfile, err)
						} else {
							logger.GetLogger("boulder").Infof("cleaned up stale tmp file (timeout): %s", tmpfile)
						}
					}
					continue
				} else if strings.HasSuffix(f.Name(), ".uploading") {
					// 正在上传中
					if time.Since(info.ModTime()) < 1*time.Minute {
						continue
					}
				}

				if time.Since(info.ModTime()) < 3*time.Second {
					// 3秒内才更新的，延迟上层
					continue
				}

				var blockID string
				if strings.HasSuffix(f.Name(), ".uploading") {
					blockID = f.Name()[:len(f.Name())-len(".uploading")]
				} else {
					blockID = f.Name()
				}

				wg.Add(1) // 启动本批次上传任务
				go func(blockid string) {
					defer wg.Done()
					sem <- struct{}{}        // 获取令牌
					defer func() { <-sem }() // 释放令牌

					blockfile := fmt.Sprintf("%s/%s", cfg.Block.UploadBufDir, blockid)
					uploadfile := fmt.Sprintf("%s/%s.uploading", cfg.Block.UploadBufDir, blockid)
					if utils.FileExists(uploadfile) && utils.FileExists(blockfile) {
						if upVer, err := readVersionFromFile(uploadfile); err == nil {
							if curVer, err := readVersionFromFile(blockfile); err == nil {
								if upVer > curVer {
									os.Remove(blockfile)
								}
							}
						}
					}

					if utils.FileExists(blockfile) {
						os.Rename(blockfile, uploadfile)
					}

					data, err := os.ReadFile(uploadfile)
					if err != nil {
						logger.GetLogger("boulder").Infof("read file %s failed %v", uploadfile, err)
						return
					}

					if len(data) < 4 {
						os.Remove(uploadfile)
						return
					}

					var block meta.Block
					if err := msgpack.Unmarshal(data[4:], &block); err != nil {
						os.Remove(uploadfile)
						return
					}

					utils.WithLockKey(block.ID, func() {
						// 再次判断文件是否存在
						if utils.FileExists(uploadfile) {
							ctx, cancel := context.WithCancel(context.Background())
							utils.WithLock(&s.cancelLocker, func() {
								s.cancelMap[block.ID] = cancel
							})

							if err := s.doFlushBlock(ctx, &block); err == nil {
								utils.WithLock(&s.cancelLocker, func() {
									delete(s.cancelMap, block.ID)
								})
								os.Remove(uploadfile)
							}
						}
					})
				}(blockID)
			}
			wg.Wait() // 等待本批次所有上传完成
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

	if chunk == nil {
		// 对象结束时候会发一个 nil chunk 表示 对象结束了，需要保存 blcok
		s.lockers[i].Lock()
		defer s.lockers[i].Unlock()
		flushBlock = s.preBlocks[i]
	} else {
		if chunk.Size != int32(len(chunk.Data)) {
			logger.GetLogger("boulder").Errorf("chunk %s/%s/%s size %d:%d not match", obj.Bucket, obj.Key, chunk.Hash, chunk.Size, len(chunk.Data))
			return nil, fmt.Errorf("chunk %s/%s/%s size %d:%d not match", obj.Bucket, obj.Key, chunk.Hash, chunk.Size, len(chunk.Data))
		}

		if chunk.Data == nil {
			logger.GetLogger("boulder").Errorf("chunk data is nil: %#v", chunk)
			return nil, fmt.Errorf("chunk %s data is nil ", chunk.Hash)
		}

		s.lockers[i].Lock()
		defer s.lockers[i].Unlock()
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
			// 块超过大小，从缓存中摘出来，保存到存储
			flushBlock = s.preBlocks[i]
			flushBlock.Finally = true
			s.preBlocks[i] = nil
		}
	}

	if flushBlock != nil {
		if flushBlock.Finally {
			flushBlock.Ver = 0x7FFF
		} else {
			flushBlock.Ver += 1
		}

		logger.GetLogger("boulder").Infof("ready to flush one block %s,  %d chunks", flushBlock.ID, len(flushBlock.ChunkList))
		err := s.FlushBlock(context.Background(), flushBlock)
		if err != nil {
			logger.GetLogger("boulder").Warnf("failed to flush block %s: %v", flushBlock.ID, err)
			return nil, fmt.Errorf("failed to flush block %s: %w", flushBlock.ID, err)
		}
	}

	return clone, nil
}

// FlushBlock
func (s *BlockService) FlushBlock(ctx context.Context, block *meta.Block) error {
	cfg := xconf.Get()
	if block.Finally {
		utils.WithLock(&s.cancelLocker, func() {
			if cancel, ok := s.cancelMap[block.ID]; ok && cancel != nil {
				// 取消正在上传的 block uploader
				cancel()
				delete(s.cancelMap, block.ID)
			}
		})
	}
	// 确保 buffer 目录存在
	if err := os.MkdirAll(cfg.Block.UploadBufDir, 0755); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to create buffer directory %s: %v", cfg.Block.UploadBufDir, err)
		return fmt.Errorf("Failed to create buffer directory %s: %w", cfg.Block.UploadBufDir, err)
	}

	filename := fmt.Sprintf("%s/%s", cfg.Block.UploadBufDir, block.ID)

	var bufferVer int32 = -1 // -1 表示文件不存在或读取失败

	// 1. 检查文件是否存在，读取当前版本号
	if ver, err := readVersionFromFile(filename); err != nil {
		bufferVer = ver
	}

	// 如果文件不存在或读取失败，bufferVer 保持 -1
	// 2. 版本比较：如果内存中的 block.Ver <= 缓冲区版本，说明是陈旧数据，丢弃
	if block.Ver <= bufferVer {
		// 陈旧数据，不写入，直接返回成功
		return nil
	}

	// 3. 准备写入新版本数据
	// 先序列化 block
	encodedBlock, err := msgpack.Marshal(block)
	if err != nil {
		logger.GetLogger("boulder").Errorf("msgpack marshal failed for block %s: %v", block.ID, err)
		return fmt.Errorf("msgpack marshal failed for block %s: %w", block.ID, err)
	}

	// 创建新数据：4字节版本号 + 序列化数据
	newData := make([]byte, 4+len(encodedBlock))
	binary.BigEndian.PutUint32(newData[0:4], uint32(block.Ver))
	copy(newData[4:], encodedBlock)

	// 4. 写入磁盘（原子性：先写临时文件，再 rename）
	tempFile := filename + ".tmp"
	if err := os.WriteFile(tempFile, newData, 0644); err != nil {
		logger.GetLogger("boulder").Errorf("write block %s temp file failed: %v", block.ID, err)
		return fmt.Errorf("write block %s temp file failed: %w", block.ID, err)
	}

	// 确保数据落盘
	if f, err := os.Open(tempFile); err == nil {
		//f.Sync()
		f.Close()
	}

	// 原子替换
	os.Remove(filename)
	if err := os.Rename(tempFile, filename); err != nil {
		logger.GetLogger("boulder").Errorf("write block %s temp file failed: %v", block.ID, err)
		return fmt.Errorf("write block %s temp file failed: %w", block.ID, err)
	}

	return nil

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
			ChunkList: make([]meta.BlockChunk, 0, len(block.ChunkList)),
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
	logger.GetLogger("boulder").Infof("flush block data %s total size %d real size %d data size %d etag %+v",
		blockData.ID, blockData.TotalSize, blockData.RealSize, len(blockData.Data), md5.Sum(blockData.Data))

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
	block.Etag = blockData.Etag

	return nil
}

func (s *BlockService) WriteBlock(ctx context.Context, storageID string, blockData *meta.BlockData) error {
	// 压缩Data
	if len(blockData.Data) > 1024 {
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

	// 计算hash, 用来做数据完整性校验
	blockData.CalcChunkHash()

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

	err = st.Instance.WriteBlock(ctx, blockData.ID, data)
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

	data, err := st.Instance.ReadBlock(blockID, 0, 0)
	if err != nil {
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
	data, err := st.Instance.ReadBlock(blockID, 0, int64(cfg.Block.MaxHeadSize))
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
			result, err := cache.BatchGet(context.Background(), batchKeys)
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
