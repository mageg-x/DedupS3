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
	"fmt"
	"sync"
)

var (
	bsInstance BlockStore
	blockOnce  sync.Once
	blockMutex sync.RWMutex
)

func InitBlockStore(id, storageType, storagePath string) (BlockStore, error) {
	var initErr error
	blockOnce.Do(func() {
		switch storageType {
		case "disk":
			bsInstance, initErr = InitDiskStore(id, storagePath)
		case "s3":
			bsInstance, initErr = InitS3Store(id, storagePath, "", "")
		default:
			initErr = fmt.Errorf("unknown storage type: %s", storageType)
		}
	})
	if initErr != nil {
		return nil, initErr
	}
	return bsInstance, nil
}

func GetBlockStore() BlockStore {
	blockMutex.RLock()
	defer blockMutex.RUnlock()
	return bsInstance
}

// BlockStore  存储后端接口
type BlockStore interface {
	ID() string
	Type() string
	WriteBlock(blockID string, data []byte) error
	ReadBlock(blockID string, offset, length int64) ([]byte, error)
	DeleteBlock(blockID string) error
	Location(blockID string) string
	Stats() StoreStats
}

// StoreStats 存储统计信息
type StoreStats struct {
	TotalSpace int64 `json:"total_space"`
	UsedSpace  int64 `json:"used_space"`
	FreeSpace  int64 `json:"free_space"`
	BlockCount int   `json:"block_count"`
}
