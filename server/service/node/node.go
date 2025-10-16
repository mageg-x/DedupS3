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
package node

import (
	"fmt"
	"github.com/mageg-x/dedups3/plugs/block"
	"github.com/mageg-x/dedups3/plugs/kv"
	"github.com/mageg-x/dedups3/service/storage"
	"sync"

	"github.com/mageg-x/dedups3/internal/config"
	"github.com/mageg-x/dedups3/internal/logger"
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
