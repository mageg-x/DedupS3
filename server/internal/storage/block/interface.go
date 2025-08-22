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
	"sync"

	"github.com/mageg-x/boulder/internal/config"
)

var (
	bsInstance BlockStore
	blockMutex sync.RWMutex
)

func GetBlockStore() BlockStore {
	blockMutex.RLock()
	defer blockMutex.RUnlock()
	if bsInstance != nil {
		return bsInstance
	}

	cfg := config.Get()
	if cfg.Block.S3 == nil {
		inst, err := NewDiskStore()
		if err == nil && inst != nil {
			bsInstance = inst
		}
	} else {
		inst, err := NewS3Store()
		if err == nil && inst != nil {
			bsInstance = inst
		}
	}

	return bsInstance
}

// BlockStore  存储后端接口
type BlockStore interface {
	Type() string
	WriteBlock(blockID string, data []byte) error
	ReadBlock(blockID string, offset, length int64) ([]byte, error)
	DeleteBlock(blockID string) error
	Location(blockID string) string
	BlockExists(blockID string) (bool, error)
}
