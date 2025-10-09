package node

import (
	"fmt"
	"github.com/mageg-x/dedups3/service/storage"
	"sync"

	"github.com/mageg-x/dedups3/internal/config"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/storage/block"
	"github.com/mageg-x/dedups3/internal/storage/kv"
)

type NodeService struct {
	kvstore kv.KVStore
}

var (
	instance *NodeService
	mu       = sync.Mutex{}
)

func GetNodeService() *NodeService {
	mu.Lock()
	defer mu.Unlock()
	if instance != nil && instance.kvstore != nil {
		return instance
	}

	store, err := kv.GetKvStore()
	if err != nil || store == nil {
		logger.GetLogger("dedups3").Errorf("failed to get kv store: %v", err)
		return nil
	}
	instance = &NodeService{
		kvstore: store,
	}
	return instance
}

func (n *NodeService) ReadLocalBlock(storageID, blockID string, offset, length int64) ([]byte, error) {
	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("dedups3").Errorf("get nil storage service")
		return nil, fmt.Errorf("get nil storage service")
	}

	st, err := ss.GetStorage(storageID)
	if err != nil || st == nil || st.Instance == nil {
		logger.GetLogger("dedups3").Errorf("get nil storage instance id %s ", storageID)
		return nil, fmt.Errorf("get nil storage instance: %w", err)
	}

	cfg := config.Get()
	ds, err := block.NewDiskStore(storageID, st.Class, &config.DiskConfig{
		Path: cfg.Node.LocalDir, // 这个路径这里用不上，随便填一个都可以，主要是利用器
	})

	if ds == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("get nil storage service")
		return nil, fmt.Errorf("get nil storage")
	}
	return ds.ReadLocalBlock(blockID, offset, length)
}
