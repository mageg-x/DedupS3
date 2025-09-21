package node

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/storage/block"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/service/storage"

	"github.com/mageg-x/boulder/internal/logger"
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
		logger.GetLogger("boulder").Errorf("failed to get kv store: %v", err)
		return nil
	}
	instance = &NodeService{
		kvstore: store,
	}
	return instance
}

func (n *NodeService) ReadLocalBlock(blockID string, offset, length int64) ([]byte, error) {
	var localPath string
	cfg := config.Get()
	ss := storage.GetStorageService()
	st, err := ss.GetStorage(storage.LOCAL_DISK_STORAGE_ID)
	if err == nil && st != nil {
		localPath = st.Conf.Disk.Path
	} else {
		localPath = filepath.Join(cfg.Node.LocalDir, "block")
	}

	ds, err := block.NewDiskStore(&config.DiskConfig{
		Path: localPath,
	})

	if ds == nil || err != nil {
		logger.GetLogger("boulder").Errorf("get nil storage service")
		return nil, fmt.Errorf("get nil storage")
	}
	return ds.ReadLocalBlock(blockID, offset, length)
}
