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
package kv

import (
	"context"
	"github.com/mageg-x/boulder/internal/config"
	"sync"
)

var (
	kvInstance KVStore
	kvMutex    sync.RWMutex
)

// GetKvStore 获取全局KV存储实例
func GetKvStore() (KVStore, error) {
	kvMutex.RLock()
	defer kvMutex.RUnlock()
	if kvInstance != nil {
		return kvInstance, nil
	}
	cfg := config.Get()
	var err error
	if cfg.KV.TiKV == nil {
		kvInstance, err = InitBadgerStore(cfg.KV.Badger)
	} else {
		kvInstance, err = InitTiKVStore(cfg.KV.TiKV)
	}

	return kvInstance, err
}

// KVStore 键值存储接口
type KVStore interface {
	Put(ctx context.Context, key string, value interface{}) error
	Get(ctx context.Context, key string, value interface{}) (bool, error)
	GetRaw(ctx context.Context, key string) ([]byte, bool, error)
	Delete(ctx context.Context, key string) error
	DeletePrefix(ctx context.Context, prefix string) error
	Scan(ctx context.Context, prefix, startKey string, limit int) ([]string, string, error)
	Close() error
}
