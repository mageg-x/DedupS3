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
	"fmt"
	"sync"
)

var (
	kvInstance KVStore
	kvOnce     sync.Once
	kvMutex    sync.RWMutex
)

func InitKvStore(cfg *Config) (KVStore, error) {
	var initErr error
	kvOnce.Do(func() {
		switch cfg.Type {
		case StorageBadger:
			kvInstance, initErr = InitBadgerStore(cfg.Badger)
		case StorageTiKV:
			kvInstance, initErr = InitTiKVStore(cfg.TiKV)
		default:
			initErr = fmt.Errorf("unsupported storage type: %s", cfg.Type)
		}
	})

	if initErr != nil {
		return nil, initErr
	}
	return kvInstance, nil
}

// GetKvStore 获取全局KV存储实例
func GetKvStore() KVStore {
	kvMutex.RLock()
	defer kvMutex.RUnlock()
	return kvInstance
}

// KVStore 键值存储接口
type KVStore interface {
	Put(ctx context.Context, key string, value interface{}) error
	Get(ctx context.Context, key string, value interface{}) (bool, error)
	GetRaw(ctx context.Context, key string) ([]byte, bool, error)
	Delete(ctx context.Context, key string) error
	Scan(ctx context.Context, prefix string) ([]string, string, error)
	ScanWithValues(ctx context.Context, prefix string) (map[string][]byte, error)
	ScanPage(ctx context.Context, prefix, startKey string, limit int) ([]string, string, error)
	Close() error

	// BeginTxn 开始一个新事务
	BeginTxn(ctx context.Context) (Txn, error)
}

// Txn 事务接口
type Txn interface {
	// Put 在事务中存储键值对
	Put(key string, value interface{}) error
	// Get 在事务中获取值
	Get(key string, value interface{}) (bool, error)
	// GetRaw 在事务中获取原始字节数据
	GetRaw(key string) ([]byte, bool, error)
	// Delete 在事务中删除键
	Delete(key string) error
	// Commit 提交事务
	Commit(ctx context.Context) error
	// Rollback 回滚事务
	Rollback(ctx context.Context) error
}

// StorageType 存储类型
type StorageType string

const (
	StorageBadger StorageType = "badger"
	StorageRedis  StorageType = "redis"
	StorageTiKV   StorageType = "tikv"
)

// Config 存储配置
type Config struct {
	Type   StorageType
	Badger BadgerConfig
	TiKV   TiKVConfig
}

// BadgerConfig Badger 存储配置
type BadgerConfig struct {
	Path string
}

// TiKVConfig TiKV 存储配置
type TiKVConfig struct {
	PDAddrs []string
}
