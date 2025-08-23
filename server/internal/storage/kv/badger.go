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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	xconf "github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
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
		logger.GetLogger("boulder").Errorf("Failed to open BadgerDB: %v", err)
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	logger.GetLogger("boulder").Infof("BadgerDB store initialized successfully")
	return &BadgerStore{db: db}, nil
}

// Put 存储键值对
func (b *BadgerStore) Put(_ context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("JSON marshal error for key %s: %v", key, err)
		return fmt.Errorf("json marshal error: %w", err)
	}

	err = b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to put key %s: %v", key, err)
	}
	return err
}

// Get 获取值并反序列化到指定结构
func (b *BadgerStore) Get(ctx context.Context, key string, value interface{}) (bool, error) {
	data, exists, err := b.GetRaw(ctx, key)
	if err != nil || !exists {
		if !exists {
			logger.GetLogger("boulder").Debugf("Key not found: %s", key)
		} else {
			logger.GetLogger("boulder").Errorf("Error getting key %s: %v", key, err)
		}
		return exists, err
	}

	if err := json.Unmarshal(data, value); err != nil {
		logger.GetLogger("boulder").Errorf("JSON unmarshal error for key %s: %v", key, err)
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	logger.GetLogger("boulder").Debugf("Successfully got value for key: %s", key)
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
				logger.GetLogger("boulder").Debugf("Key not found: %s", key)
				return nil
			}
			logger.GetLogger("boulder").Errorf("Error getting raw data for key %s: %v", key, err)
			return err
		}

		exists = true
		return item.Value(func(val []byte) error {
			data = make([]byte, len(val))
			copy(data, val)
			return nil
		})
	})

	if err == nil && exists {
		logger.GetLogger("boulder").Debugf("Successfully got raw data for key: %s", key)
	}
	return data, exists, err
}

// Delete 删除键
func (b *BadgerStore) Delete(_ context.Context, key string) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", key, err)
	} else {
		logger.GetLogger("boulder").Debugf("Successfully deleted key: %s", key)
	}
	return err
}

func (b *BadgerStore) DeletePrefix(ctx context.Context, prefix string) error {
	err := b.db.DropPrefix([]byte(prefix))
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", prefix, err)
	} else {
		logger.GetLogger("boulder").Debugf("Successfully deleted key: %s", prefix)
	}
	return err
}

func (b *BadgerStore) Scan(ctx context.Context, prefix, startKey string, limit int) ([]string, string, error) {
	var keys []string
	var nextKey string

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // 我们只需要键，不需要值
		opts.PrefetchSize = 100     // 预取大小，可根据需要调整

		it := txn.NewIterator(opts)
		defer it.Close()

		// 确定起始位置
		seekKey := []byte(startKey)
		if startKey == "" {
			seekKey = []byte(prefix)
		}
		// 开始迭代
		it.Seek(seekKey)

		count := 0
		prefixBytes := []byte(prefix)
		prefixLen := len(prefixBytes)

		for ; it.Valid() && count < limit; it.Next() {
			item := it.Item()
			key := item.Key()

			// 检查是否仍然在指定前缀范围内
			if len(key) < prefixLen || !bytes.Equal(key[:prefixLen], prefixBytes) {
				break
			}

			// 转换为字符串并添加到结果
			keyStr := string(key)
			keys = append(keys, keyStr)
			count++

			// 检查上下文是否已取消
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		// 获取下一个键（如果有）
		if it.Valid() {
			item := it.Item()
			nextKeyBytes := item.Key()

			// 确保下一个键仍然在前缀范围内
			if len(nextKeyBytes) >= prefixLen && bytes.Equal(nextKeyBytes[:prefixLen], prefixBytes) {
				nextKey = string(nextKeyBytes)
			}
		}

		return nil
	})

	if err != nil {
		return nil, "", err
	}

	return keys, nextKey, nil
}

// Close 关闭数据库
func (b *BadgerStore) Close() error {
	if b.db == nil {
		logger.GetLogger("boulder").Errorf("Database already closed")
		return errors.New("database already closed")
	}

	if err := b.db.Close(); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to close BadgerDB: %v", err)
		return fmt.Errorf("failed to close badger db: %w", err)
	}

	logger.GetLogger("boulder").Debugf("BadgerDB store closed successfully")
	return nil
}
