package block

import (
	"sync"

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

func (s *BlockService) PutChunk(storageID, objKey string, chunk *meta.Chunk) (string, bool, error) {
	return "", false, nil
}
func (s *BlockService) FlushBlock(block *meta.Block) error {
	return nil
}
func (s *BlockService) FlushBlockByID(blockID string) error {
	return nil
}
