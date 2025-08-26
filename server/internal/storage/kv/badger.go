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

// BadgerTxn 基于BadgerDB的事务实现
type BadgerTxn struct {
	txn *badger.Txn
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

func (b *BadgerStore) Get(key string, value interface{}) (bool, error) {
	txn, err := b.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize TiKV txn: %v", err)
		return false, err
	}
	defer txn.Rollback()
	return txn.Get(key, value)
}

func (b *BadgerStore) GetRaw(key string) ([]byte, bool, error) {
	txn, err := b.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize TiKV txn: %v", err)
		return nil, false, err
	}
	defer txn.Rollback()
	return txn.GetRaw(key)
}

func (b *BadgerStore) BatchGet(keys []string) (map[string][]byte, error) {
	txn, err := b.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize TiKV txn: %v", err)
		return nil, err
	}
	defer txn.Rollback()
	return txn.BatchGet(keys)
}

// BeginTxn 开始一个新事务
func (b *BadgerStore) BeginTxn(_ context.Context, _ *TxnOpt) (Txn, error) {
	txn := b.db.NewTransaction(true)
	return &BadgerTxn{txn: txn}, nil
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

// Get 在事务中获取值
func (t *BadgerTxn) Get(key string, value interface{}) (bool, error) {
	data, exists, err := t.GetRaw(key)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Error getting key %s: %v", key, err)
		return false, err
	}
	if !exists {
		logger.GetLogger("boulder").Debugf("Key not found: %s", key)
		return false, fmt.Errorf("key not found: %s", key)
	}

	if err := json.Unmarshal(data, value); err != nil {
		logger.GetLogger("boulder").Errorf("JSON unmarshal error for key %s: %v", key, err)
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	logger.GetLogger("boulder").Debugf("Successfully got value for key: %s", key)
	return true, nil
}

// GetRaw 在事务中获取原始字节数据
func (t *BadgerTxn) GetRaw(key string) ([]byte, bool, error) {
	var data []byte
	item, err := t.txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			logger.GetLogger("boulder").Debugf("Key not found: %s", key)
			return nil, false, fmt.Errorf("key not found: %s", key)
		}
		return nil, false, err
	}

	err = item.Value(func(val []byte) error {
		data = make([]byte, len(val))
		copy(data, val)
		return nil
	})
	if err == nil {
		logger.GetLogger("boulder").Debugf("Successfully got raw data for key: %s", key)
	}
	return data, true, err
}

func (t *BadgerTxn) BatchGet(keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte, 0)
	for _, key := range keys {
		item, err := t.txn.Get([]byte(key)) // 注意：Badger 需要 []byte 类型的键
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				// key 不存在，跳过
				continue
			}
			// 处理其他错误
			return nil, fmt.Errorf("failed to get key %s: %w", key, err)
		}

		// 从项中提取值
		err = item.Value(func(val []byte) error {
			// 复制值到结果映射中（因为val只在当前函数调用中有效）
			valueCopy := make([]byte, len(val))
			copy(valueCopy, val)
			result[key] = valueCopy
			return nil
		})

		if err != nil {
			return nil, fmt.Errorf("failed to extract value for key %s: %w", key, err)
		}
	}
	return result, nil
}

func (t *BadgerTxn) Set(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("JSON marshal error for key %s: %v", key, err)
		return fmt.Errorf("json marshal error: %w", err)
	}
	err = t.txn.Set([]byte(key), data)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to put key %s: %v", key, err)
	}
	return err
}

func (t *BadgerTxn) SetNX(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("JSON marshal error for key %s: %v", key, err)
		return fmt.Errorf("json marshal error: %w", err)
	}
	_, err = t.txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		// 键不存在，可以设置
		return t.txn.Set([]byte(key), data)
	} else if err != nil {
		// 处理其他错误
		return err
	}
	// 键已存在，不进行操作并返回特定错误或标识
	return ErrKeyExists
}

func (t *BadgerTxn) BatchSet(kvs map[string]interface{}) error {
	// 遍历所有键值对
	for key, value := range kvs {
		// 序列化值
		data, err := json.Marshal(value)
		if err != nil {
			logger.GetLogger("boulder").Errorf("JSON marshal error for key %s: %v", key, err)
			return fmt.Errorf("json marshal error for key %s: %w", key, err)
		}

		// 设置键值对
		err = t.txn.Set([]byte(key), data)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to set key %s: %v", key, err)
			return fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return nil
}

func (t *BadgerTxn) Delete(key string) error {
	err := t.txn.Delete([]byte(key))
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", key, err)
	}
	return err
}

func (t *BadgerTxn) DeletePrefix(prefix string, limit int32) error {
	// 创建迭代选项，只迭代指定前缀的键
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte(prefix)

	// 创建迭代器
	it := t.txn.NewIterator(opts)
	defer it.Close()

	count := int32(0)
	// 遍历所有匹配前缀的键
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		count += 1
		if count > limit {
			break
		}
		// 删除键
		err := t.txn.Delete(key)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", string(key), err)
			return fmt.Errorf("failed to delete key %s: %w", string(key), err)
		}
	}

	return nil
}

func (t *BadgerTxn) Scan(prefix string, startKey string, limit int) ([]string, string, error) {
	var keys []string
	var nextKey string

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // 我们只需要键，不需要值
	opts.PrefetchSize = 100     // 预取大小，可根据需要调整

	it := t.txn.NewIterator(opts)
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
	return keys, nextKey, nil
}

// Commit 提交事务
func (t *BadgerTxn) Commit() error {
	return t.txn.Commit()
}

// Rollback 回滚事务
func (t *BadgerTxn) Rollback() error {
	t.txn.Discard()
	return nil
}
