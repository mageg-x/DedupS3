package block

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
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

const (
	PRE_UPLOAD_BLOCK_NUM   = 16
	MAX_BUCKET_HEADER_SIZE = 200 * 1024
	MAX_BUCKET_SIZE        = 64 * 1024 * 1024
)

var (
	instance *BlockService
	mu       = sync.Mutex{}
)

type BlockService struct {
	kvstore kv.KVStore

	preBlocks  []*meta.Block
	blockLocks []sync.Mutex // 为每个块单独设置锁
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
	instance = &BlockService{
		kvstore:    store,
		preBlocks:  make([]*meta.Block, PRE_UPLOAD_BLOCK_NUM),
		blockLocks: make([]sync.Mutex, PRE_UPLOAD_BLOCK_NUM),
	}

	return instance
}

func (s *BlockService) PutChunk(chunk *meta.Chunk, obj *meta.BaseObject) (*meta.Block, error) {
	h := murmur3.Sum32([]byte(obj.Bucket + obj.Key))
	i := h % PRE_UPLOAD_BLOCK_NUM
	var flushBlock *meta.Block

	if chunk == nil {
		// 对象结束时候会发一个 nil chunk 表示 对象结束了，需要保存 blcok
		s.blockLocks[i].Lock()
		defer s.blockLocks[i].Unlock()
		flushBlock = s.preBlocks[i]
	} else {
		if chunk.Data == nil {
			logger.GetLogger("boulder").Errorf("chunk data is nil: %#v", chunk)
			return nil, fmt.Errorf("chunk data is nil: %#v", chunk)
		}
		s.blockLocks[i].Lock()
		defer s.blockLocks[i].Unlock()
		curBlock := s.preBlocks[i]
		if curBlock == nil {
			curBlock = meta.NewBlock(obj.DataLocation)
			s.preBlocks[i] = curBlock
		}
		chunk.BlockID = curBlock.ID
		curBlock.ChunkList = append(curBlock.ChunkList, meta.BlockChunk{Hash: chunk.Hash, Size: chunk.Size, Data: chunk.Data})
		chunk.Data = nil
		curBlock.TotalSize += int64(chunk.Size)
		curBlock.UpdatedAt = time.Now()
		if curBlock.TotalSize > MAX_BUCKET_SIZE {
			// 块超过大小，从缓存中摘出来，保存到存储
			flushBlock = curBlock
			s.preBlocks[i] = nil
		}
	}

	if flushBlock != nil {
		logger.GetLogger("boulder").Warnf("ready to flush one block %s,  %d chunks", flushBlock.ID, len(flushBlock.ChunkList))
		err := s.FlushBlock(flushBlock)
		if err != nil {
			logger.GetLogger("boulder").Warnf("failed to flush block %s: %v", flushBlock.ID, err)
			return nil, fmt.Errorf("failed to flush block %s: %v", flushBlock.ID, err)
		}
	}
	return flushBlock, nil
}

func (s *BlockService) FlushBlock(block *meta.Block) error {
	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("boulder").Errorf("get nil storage service")
		return fmt.Errorf("get nil storage service")
	}

	st, err := ss.GetStorage(block.StorageID)
	if err != nil || st == nil || st.Instance == nil {
		logger.GetLogger("boulder").Errorf("get nil storage instance")
		return fmt.Errorf("get nil storage instance")
	}

	blockData := meta.BlockData{
		BlockHeader: meta.BlockHeader{
			ID:        block.ID,
			TotalSize: block.TotalSize,
			ChunkList: make([]meta.BlockChunk, len(block.ChunkList)),
		},

		Data: make([]byte, 0),
	}

	for i := 0; i < len(block.ChunkList); i++ {
		_chunk := meta.BlockChunk{
			Hash: block.ChunkList[i].Hash,
			Size: block.ChunkList[i].Size,
		}
		blockData.ChunkList = append(blockData.ChunkList, _chunk)
		blockData.Data = append(blockData.Data, block.ChunkList[i].Data...)
		block.ChunkList[i].Data = nil
	}
	blockData.RealSize = block.TotalSize
	block.RealSize = block.TotalSize

	logger.GetLogger("boulder").Infof("flush block data %s total size %d real size %d etag %+v", blockData.ID, blockData.TotalSize, blockData.RealSize, md5.Sum(blockData.Data))

	// 压缩Data
	if len(blockData.Data) > 1024 && utils.IsCompressible(blockData.Data, 64*1024, 0.9) {
		compress, err := utils.Compress(blockData.Data)
		if err == nil && compress != nil {
			block.Compressed = true
			block.RealSize = int64(len(compress))
			blockData.Data = compress
			blockData.Compressed = true
			blockData.RealSize = int64(len(compress))
		}
	}

	// 加密Data
	encrypt, err := utils.Encrypt(blockData.Data, blockData.ID)
	if err == nil && encrypt != nil {
		block.Encrypted = true
		block.RealSize = int64(len(encrypt))
		blockData.Data = encrypt
		blockData.Encrypted = true
		blockData.RealSize = int64(len(encrypt))
	}

	logger.GetLogger("boulder").Infof("flush block data size %d:%d, compress rate %.2f%%",
		blockData.TotalSize, blockData.RealSize, float64(100.0*blockData.RealSize)/float64(blockData.TotalSize))

	data, err := msgpack.Marshal(&blockData)
	if err != nil {
		logger.GetLogger("boulder").Debugf("msgpack marshal %s failed: %v", block.ID, err)
		return fmt.Errorf("msgpack marshal %s failed: %v", block.ID, err)
	}

	err = st.Instance.WriteBlock(block.ID, data)
	if err != nil {
		logger.GetLogger("boulder").Debugf("write block %s failed: %v", block.ID, err)
		return fmt.Errorf("write block %s failed: %v", block.ID, err)
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
		return nil, fmt.Errorf("get nil storage instance")
	}

	data, err := st.Instance.ReadBlock(blockID, 0, 0)
	if err != nil {
		logger.GetLogger("boulder").Errorf("read block %s failed: %v", blockID, err)
		return nil, fmt.Errorf("read block %s failed: %v", blockID, err)
	}
	blockData := meta.BlockData{}
	err = msgpack.Unmarshal(data, &blockData)
	if err != nil {
		logger.GetLogger("boulder").Errorf("msgpack unmarshal block %s  data %d to struct failed: %v", blockID, len(data), err)
		return nil, fmt.Errorf("msgpack unmarshal block %s  data %d to struct failed: %v", blockID, len(data), err)
	}
	if blockID != blockData.ID {
		logger.GetLogger("boulder").Errorf("read block %s id not match block %s ", blockID, blockData.ID)
		return nil, fmt.Errorf("read block %s id not match block %s ", blockID, blockData.ID)
	}
	if blockData.Encrypted {
		_d, err := utils.Decrypt(blockData.Data, blockID)
		if err != nil {
			logger.GetLogger("boulder").Errorf("decrypt block %s failed: %v", blockID, err)
			return nil, fmt.Errorf("decrypt block %s failed: %v", blockID, err)
		}
		blockData.Data = _d
	}

	if blockData.Compressed {
		_d, err := utils.Decompress(blockData.Data)
		if err != nil {
			logger.GetLogger("boulder").Errorf("decompress block %s data failed: %v", blockID, err)
			return nil, fmt.Errorf("decompress block %s data failed: %v", blockID, err)
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
		return nil, fmt.Errorf("get nil storage instance")
	}

	data, err := st.Instance.ReadBlock(blockID, 0, MAX_BUCKET_HEADER_SIZE)
	if err != nil {
		logger.GetLogger("boulder").Errorf("read block header %s failed: %v", blockID, err)
		return nil, fmt.Errorf("read block header %s failed: %v", blockID, err)
	}

	dec := msgpack.NewDecoder(bytes.NewReader(data))
	var header meta.BlockHeader
	err = dec.Decode(&header)
	if err != nil {
		logger.GetLogger("boulder").Errorf("decode block header %s failed: %v", blockID, err)
		return nil, fmt.Errorf("decode block header %s failed: %v", blockID, err)
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
		return fmt.Errorf("get nil storage instance")
	}

	err = st.Instance.DeleteBlock(blockID)
	if err != nil {
		if errors.Is(err, sb.ErrBlockNotFound) {
			return nil
		}
		logger.GetLogger("boulder").Debugf("failed to remove block %s: %v", blockID, err)
		return fmt.Errorf("failed to remove block %s: %v", blockID, err)
	}
	return nil
}

func (s *BlockService) BatchGet(storageID string, blockIds []string) ([]*meta.Block, error) {
	blockMap := make(map[string]*meta.Block)
	keys := make([]string, 0, len(blockIds))
	for _, id := range blockIds {
		key := "aws:block:" + storageID + ":" + id
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
			return nil, fmt.Errorf("failed to batchGet blocks: %v", err)
		}
		for k, v := range result {
			var block meta.Block
			err := json.Unmarshal(v, &block)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to Unmarshal block %s err: %v", k, err)
				return nil, fmt.Errorf("failed to Unmarshal block %s err: %v", k, err)
			}
			blockMap[block.ID] = &block

			if cache, err := xcache.GetCache(); err == nil && cache != nil {
				blockKey := "aws:block:" + storageID + ":" + block.ID
				err := cache.Set(context.Background(), blockKey, &block, time.Hour*24*7)
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
