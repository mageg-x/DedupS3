package block

import (
	"fmt"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/service/storage"
	"github.com/twmb/murmur3"
	"github.com/vmihailenco/msgpack/v5"
	"sync"
	"time"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/meta"
)

const (
	PRE_UPLOAD_BLOCK_NUM = 16
)

var (
	instance *BlockService
	mu       = sync.Mutex{}
)

type BlockService struct {
	kvstore kv.KVStore

	preBlocks []*meta.Block
	muxtext   sync.Mutex
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
		kvstore:   store,
		preBlocks: make([]*meta.Block, PRE_UPLOAD_BLOCK_NUM),
		muxtext:   sync.Mutex{},
	}

	return instance
}

func (s *BlockService) PutChunk(chunk *meta.Chunk, storageID, bucket, objKey string) (*meta.Block, error) {
	h := murmur3.Sum32([]byte(bucket + objKey))
	i := h % PRE_UPLOAD_BLOCK_NUM
	var flushBlock *meta.Block

	if chunk == nil {
		// 结束了
		utils.WithLock(&s.muxtext, func() {
			flushBlock = s.preBlocks[i]
		})
	} else {
		utils.WithLock(&s.muxtext, func() {
			curBlock := s.preBlocks[i]
			if curBlock == nil {
				curBlock = meta.NewBlock(storageID)
				s.preBlocks[i] = curBlock
			}
			chunk.BlockID = curBlock.ID
			curBlock.ChunkList = append(curBlock.ChunkList, meta.BlockChunk{Hash: chunk.Hash, Size: chunk.Size, Data: chunk.Data})
			curBlock.TotalSize += int64(chunk.Size)
			curBlock.UpdatedAt = time.Now()
			if curBlock.TotalSize > 64*1024*1024 {
				flushBlock = curBlock
				s.preBlocks[i] = nil
			}
		})
	}

	if flushBlock != nil {
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
		ID:        block.ID,
		TotalSize: block.TotalSize,
		ChunkList: make([]meta.BlockChunk, 0),
		Data:      make([]byte, 0),
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

	logger.GetLogger("boulder").Debugf("pre flush block data size %d:%d", blockData.TotalSize, len(blockData.Data))

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

func (s *BlockService) RemoveBlock(block *meta.Block) error {
	return nil
}
