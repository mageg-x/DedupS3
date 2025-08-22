// Package kv /*
package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
	"os"
	"path/filepath"
	"time"
)

// BadgerStore 实现基于BadgerDB的KV存储
type BadgerStore struct {
	db     *badger.DB
	ctx    context.Context
	closed bool
}

// InitBadgerStore 初始化BadgerDB存储
func InitBadgerStore(c *config.KVConfig) (*BadgerStore, error) {
	logger.GetLogger("boulder").Infof("Initializing BadgerDB store at path: %s", c.Path)
	if c == nil {
		logger.GetLogger("boulder").Errorf("InitBadgerStore: nil config")
		return nil, fmt.Errorf("InitBadgerStore: nil config")
	}

	// 确保数据目录存在
	if err := os.MkdirAll(c.Path, 0755); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to create BadgerDB directory: %v", err)
		return nil, fmt.Errorf("failed to create BadgerDB directory: %w", err)
	}

	// 配置BadgerDB
	opts := badger.DefaultOptions(c.Path)
	if c.ReadOnly {
		opts = opts.WithReadOnly(true)
		logger.GetLogger("boulder").Debugf("BadgerDB opened in read-only mode")
	}
	if c.MemoryOnly {
		opts = opts.WithInMemory(true)
		logger.GetLogger("boulder").Debugf("BadgerDB opened in memory-only mode")
	}

	// 打开BadgerDB
	db, err := badger.Open(opts)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to open BadgerDB: %v", err)
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	logger.GetLogger("boulder").Infof("BadgerDB store initialized successfully")
	return &BadgerStore{
		db:     db,
		ctx:    context.Background(),
		closed: false,
	}, nil
}

// Type 返回存储类型
func (b *BadgerStore) Type() string {
	return "badger"
}

// Put 写入键值对
func (b *BadgerStore) Put(key string, value interface{}) error {
	logger.GetLogger("boulder").Debugf("Putting key: %s", key)
	if b.closed {
		logger.GetLogger("boulder").Errorf("Put operation on closed BadgerDB store")
		return errors.New("badger store is closed")
	}

	// 序列化值
	bytes, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to marshal value for key %s: %v", key, err)
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	// 写入数据库
	err = b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), bytes)
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to put key %s: %v", key, err)
		return fmt.Errorf("failed to put key %s: %w", key, err)
	}

	logger.GetLogger("boulder").Debugf("Key %s put successfully", key)
	return nil
}

// Get 读取键值对
func (b *BadgerStore) Get(key string, value interface{}) (bool, error) {
	logger.GetLogger("boulder").Debugf("Getting key: %s", key)
	if b.closed {
		logger.GetLogger("boulder").Errorf("Get operation on closed BadgerDB store")
		return false, errors.New("badger store is closed")
	}

	var data []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				logger.GetLogger("boulder").Debugf("Key %s not found", key)
				return nil
			}
			logger.GetLogger("boulder").Errorf("Failed to get key %s: %v", key, err)
			return err
		}

		// 复制值
		data, err = item.ValueCopy(nil)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to copy value for key %s: %v", key, err)
			return err
		}
		return nil
	})

	if err != nil {
		logger.GetLogger("boulder").Errorf("Error during get operation for key %s: %v", key, err)
		return false, fmt.Errorf("error during get operation: %w", err)
	}

	if data == nil {
		logger.GetLogger("boulder").Debugf("Key %s not found in BadgerDB", key)
		return false, nil
	}

	// 反序列化值
	err = json.Unmarshal(data, value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to unmarshal value for key %s: %v", key, err)
		return false, fmt.Errorf("failed to unmarshal value: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Key %s retrieved successfully", key)
	return true, nil
}

// Delete 删除键值对
func (b *BadgerStore) Delete(key string) error {
	logger.GetLogger("boulder").Debugf("Deleting key: %s", key)
	if b.closed {
		logger.GetLogger("boulder").Errorf("Delete operation on closed BadgerDB store")
		return errors.New("badger store is closed")
	}

	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", key, err)
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}

	logger.GetLogger("boulder").Debugf("Key %s deleted successfully", key)
	return nil
}

// BatchPut 批量写入键值对
func (b *BadgerStore) BatchPut(items map[string]interface{}) error {
	logger.GetLogger("boulder").Debugf("Batch putting %d items", len(items))
	if b.closed {
		logger.GetLogger("boulder").Errorf("BatchPut operation on closed BadgerDB store")
		return errors.New("badger store is closed")
	}

	err := b.db.Update(func(txn *badger.Txn) error {
		for key, value := range items {
			bytes, err := json.Marshal(value)
			if err != nil {
				logger.GetLogger("boulder").Errorf("Failed to marshal value for key %s in batch put: %v", key, err)
				return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
			}

			if err := txn.Set([]byte(key), bytes); err != nil {
				logger.GetLogger("boulder").Errorf("Failed to put key %s in batch operation: %v", key, err)
				return fmt.Errorf("failed to put key %s: %w", key, err)
			}
		}
		return nil
	})

	if err != nil {
		logger.GetLogger("boulder").Errorf("Batch put operation failed: %v", err)
		return fmt.Errorf("batch put operation failed: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Batch put %d items successfully", len(items))
	return nil
}

// BatchDelete 批量删除键值对
func (b *BadgerStore) BatchDelete(keys []string) error {
	logger.GetLogger("boulder").Debugf("Batch deleting %d keys", len(keys))
	if b.closed {
		logger.GetLogger("boulder").Errorf("BatchDelete operation on closed BadgerDB store")
		return errors.New("badger store is closed")
	}

	err := b.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete([]byte(key)); err != nil {
				logger.GetLogger("boulder").Errorf("Failed to delete key %s in batch operation: %v", key, err)
				return fmt.Errorf("failed to delete key %s: %w", key, err)
			}
		}
		return nil
	})

	if err != nil {
		logger.GetLogger("boulder").Errorf("Batch delete operation failed: %v", err)
		return fmt.Errorf("batch delete operation failed: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Batch deleted %d keys successfully", len(keys))
	return nil
}

// Close 关闭数据库
func (b *BadgerStore) Close() error {
	logger.GetLogger("boulder").Infof("Closing BadgerDB store")
	if b.closed {
		logger.GetLogger("boulder").Debugf("BadgerDB store already closed")
		return nil
	}

	err := b.db.Close()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to close BadgerDB: %v", err)
		return fmt.Errorf("failed to close BadgerDB: %w", err)
	}

	b.closed = true
	logger.GetLogger("boulder").Infof("BadgerDB store closed successfully")
	return nil
}

// Compact 压缩数据库
func (b *BadgerStore) Compact() error {
	logger.GetLogger("boulder").Infof("Compacting BadgerDB store")
	if b.closed {
		logger.GetLogger("boulder").Errorf("Compact operation on closed BadgerDB store")
		return errors.New("badger store is closed")
	}

	err := b.db.RunValueLogGC(0.5)
	if err != nil {
		if err == badger.ErrNoRewrite {
			logger.GetLogger("boulder").Debugf("No BadgerDB compaction needed")
			return nil
		}
		logger.GetLogger("boulder").Errorf("Failed to compact BadgerDB: %v", err)
		return fmt.Errorf("failed to compact BadgerDB: %w", err)
	}

	logger.GetLogger("boulder").Infof("BadgerDB compaction completed successfully")
	return nil
}
