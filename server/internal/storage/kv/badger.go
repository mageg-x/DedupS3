/*
 *Copyright (C) 2025-2025 raochaoxun <raochaoxun@gmail.com>.
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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	xconf "github.com/mageg-x/boulder/internal/config"
)

// BadgerStore 基于BadgerDB的KV存储实现
type BadgerStore struct {
	db *badger.DB
}

// InitBadgerStore 初始化Badger存储
func InitBadgerStore(cfg xconf.BadgerConfig) (*BadgerStore, error) {
	opts := badger.DefaultOptions(cfg.Path)
	opts.Logger = nil // 禁用日志

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	return &BadgerStore{db: db}, nil
}

// Put 存储键值对
func (b *BadgerStore) Put(_ context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}

	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// Get 获取值并反序列化到指定结构
func (b *BadgerStore) Get(ctx context.Context, key string, value interface{}) (bool, error) {
	data, exists, err := b.GetRaw(ctx, key)
	if err != nil || !exists {
		return exists, err
	}

	if err := json.Unmarshal(data, value); err != nil {
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	return true, nil
}

// GetRaw 获取原始字节数据
func (b *BadgerStore) GetRaw(_ context.Context, key string) ([]byte, bool, error) {
	var data []byte
	var exists bool

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}

		exists = true
		return item.Value(func(val []byte) error {
			data = make([]byte, len(val))
			copy(data, val)
			return nil
		})
	})

	return data, exists, err
}

// Delete 删除键
func (b *BadgerStore) Delete(_ context.Context, key string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// Scan 扫描指定前缀的键
func (b *BadgerStore) Scan(_ context.Context, prefix string) ([]string, string, error) {
	return b.scanInternal(prefix, "", 0)
}

// ScanWithValues 扫描指定前缀的键值对
func (b *BadgerStore) ScanWithValues(_ context.Context, prefix string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			err := item.Value(func(val []byte) error {
				value := make([]byte, len(val))
				copy(value, val)
				result[key] = value
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})

	return result, err
}

// ScanPage 分页扫描键
func (b *BadgerStore) ScanPage(_ context.Context, prefix, startKey string, limit int) ([]string, string, error) {
	return b.scanInternal(prefix, startKey, limit)
}

// scanInternal 内部扫描实现
func (b *BadgerStore) scanInternal(prefix, startKey string, limit int) ([]string, string, error) {
	var keys []string
	var nextKey string

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		opts.PrefetchValues = false // 只获取键

		it := txn.NewIterator(opts)
		defer it.Close()

		// 定位到起始键
		if startKey != "" {
			it.Seek([]byte(startKey))
			if it.Valid() && string(it.Item().Key()) == startKey {
				it.Next() // 跳过起始键本身
			}
		} else {
			it.Rewind()
		}

		count := 0
		for ; it.Valid(); it.Next() {
			key := string(it.Item().Key())

			// 检查是否超出前缀范围
			if !strings.HasPrefix(key, prefix) {
				break
			}

			keys = append(keys, key)
			count++

			// 如果设置了限制且达到限制，获取下一个键
			if limit > 0 && count >= limit {
				it.Next()
				if it.Valid() {
					nextKey = string(it.Item().Key())
				}
				break
			}
		}
		return nil
	})

	return keys, nextKey, err
}

// Close 关闭数据库
func (b *BadgerStore) Close() error {
	if b.db == nil {
		return errors.New("database already closed")
	}

	if err := b.db.Close(); err != nil {
		return fmt.Errorf("failed to close badger db: %w", err)
	}

	return nil
}

// BeginTxn 开始一个新事务
func (b *BadgerStore) BeginTxn(_ context.Context) (Txn, error) {
	txn := b.db.NewTransaction(true)
	return &BadgerTxn{txn: txn}, nil
}

// BadgerTxn 基于BadgerDB的事务实现
type BadgerTxn struct {
	txn *badger.Txn
}

// Put 在事务中存储键值对
func (t *BadgerTxn) Put(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}

	return t.txn.Set([]byte(key), data)
}

// Get 在事务中获取值
func (t *BadgerTxn) Get(key string, value interface{}) (bool, error) {
	data, exists, err := t.GetRaw(key)
	if err != nil || !exists {
		return exists, err
	}

	if err := json.Unmarshal(data, value); err != nil {
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	return true, nil
}

// GetRaw 在事务中获取原始字节数据
func (t *BadgerTxn) GetRaw(key string) ([]byte, bool, error) {
	var data []byte
	var exists bool

	item, err := t.txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	exists = true
	err = item.Value(func(val []byte) error {
		data = make([]byte, len(val))
		copy(data, val)
		return nil
	})

	return data, exists, err
}

// Delete 在事务中删除键
func (t *BadgerTxn) Delete(key string) error {
	return t.txn.Delete([]byte(key))
}

// Commit 提交事务
func (t *BadgerTxn) Commit(_ context.Context) error {
	return t.txn.Commit()
}

// Rollback 回滚事务
func (t *BadgerTxn) Rollback(_ context.Context) error {
	t.txn.Discard()
	return nil
}
