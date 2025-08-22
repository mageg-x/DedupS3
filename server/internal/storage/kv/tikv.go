// Package kv /*
package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
	"time"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

// TiKVStore 实现基于TiKV的KV存储
type TiKVStore struct {
	client *rawkv.Client
	ctx    context.Context
	closed bool
}

// InitTiKVStore 初始化TiKV存储
func InitTiKVStore(c *config.KVConfig) (*TiKVStore, error) {
	logger.GetLogger("boulder").Infof("Initializing TiKV store with PD endpoints: %v", c.PDEndpoints)
	if c == nil {
		logger.GetLogger("boulder").Errorf("InitTiKVStore: nil config")
		return nil, fmt.Errorf("InitTiKVStore: nil config")
	}

	ctx := context.Background()

	// 创建TiKV配置
	conf := config.DefaultConfig()
	conf.Security.CAPath = c.CAPath
	conf.Security.CertPath = c.CertPath
	conf.Security.KeyPath = c.KeyPath

	// 创建TiKV原始客户端
	client, err := rawkv.NewClient(ctx, c.PDEndpoints, conf)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to create TiKV client: %v", err)
		return nil, fmt.Errorf("failed to create TiKV client: %w", err)
	}

	logger.GetLogger("boulder").Infof("TiKV store initialized successfully")
	return &TiKVStore{
		client: client,
		ctx:    ctx,
		closed: false,
	}, nil
}

// Type 返回存储类型
func (t *TiKVStore) Type() string {
	return "tikv"
}

// Put 写入键值对
func (t *TiKVStore) Put(key string, value interface{}) error {
	logger.GetLogger("boulder").Debugf("Putting key: %s", key)
	if t.closed {
		logger.GetLogger("boulder").Errorf("Put operation on closed TiKV store")
		return errors.New("tikv store is closed")
	}

	// 序列化值
	bytes, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to marshal value for key %s: %v", key, err)
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	// 设置超时上下文
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()

	// 写入数据
	err = t.client.Put(ctx, []byte(key), bytes)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to put key %s: %v", key, err)
		return fmt.Errorf("failed to put key %s: %w", key, err)
	}

	logger.GetLogger("boulder").Debugf("Key %s put successfully", key)
	return nil
}

// Get 读取键值对
func (t *TiKVStore) Get(key string, value interface{}) (bool, error) {
	logger.GetLogger("boulder").Debugf("Getting key: %s", key)
	if t.closed {
		logger.GetLogger("boulder").Errorf("Get operation on closed TiKV store")
		return false, errors.New("tikv store is closed")
	}

	// 设置超时上下文
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()

	// 读取数据
	data, err := t.client.Get(ctx, []byte(key))
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to get key %s: %v", key, err)
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	if data == nil {
		logger.GetLogger("boulder").Debugf("Key %s not found in TiKV", key)
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
func (t *TiKVStore) Delete(key string) error {
	logger.GetLogger("boulder").Debugf("Deleting key: %s", key)
	if t.closed {
		logger.GetLogger("boulder").Errorf("Delete operation on closed TiKV store")
		return errors.New("tikv store is closed")
	}

	// 设置超时上下文
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()

	// 删除数据
	err = t.client.Delete(ctx, []byte(key))
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", key, err)
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}

	logger.GetLogger("boulder").Debugf("Key %s deleted successfully", key)
	return nil
}

// BatchPut 批量写入键值对
func (t *TiKVStore) BatchPut(items map[string]interface{}) error {
	logger.GetLogger("boulder").Debugf("Batch putting %d items", len(items))
	if t.closed {
		logger.GetLogger("boulder").Errorf("BatchPut operation on closed TiKV store")
		return errors.New("tikv store is closed")
	}

	// 准备键值对
	kvs := make([]rawkv.KV, 0, len(items))
	for key, value := range items {
		bytes, err := json.Marshal(value)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to marshal value for key %s in batch put: %v", key, err)
			return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}
		kvs = append(kvs, rawkv.KV{
			Key:   []byte(key),
			Value: bytes,
		})
	}

	// 设置超时上下文
	ctx, cancel := context.WithTimeout(t.ctx, 30*time.Second)
	defer cancel()

	// 批量写入
	err := t.client.BatchPut(ctx, kvs)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Batch put operation failed: %v", err)
		return fmt.Errorf("batch put operation failed: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Batch put %d items successfully", len(items))
	return nil
}

// BatchDelete 批量删除键值对
func (t *TiKVStore) BatchDelete(keys []string) error {
	logger.GetLogger("boulder").Debugf("Batch deleting %d keys", len(keys))
	if t.closed {
		logger.GetLogger("boulder").Errorf("BatchDelete operation on closed TiKV store")
		return errors.New("tikv store is closed")
	}

	// 转换键为字节数组
	byteKeys := make([][]byte, 0, len(keys))
	for _, key := range keys {
		byteKeys = append(byteKeys, []byte(key))
	}

	// 设置超时上下文
	ctx, cancel := context.WithTimeout(t.ctx, 30*time.Second)
	defer cancel()

	// 批量删除
	err := t.client.BatchDelete(ctx, byteKeys)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Batch delete operation failed: %v", err)
		return fmt.Errorf("batch delete operation failed: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Batch deleted %d keys successfully", len(keys))
	return nil
}

// Close 关闭客户端
func (t *TiKVStore) Close() error {
	logger.GetLogger("boulder").Infof("Closing TiKV store")
	if t.closed {
		logger.GetLogger("boulder").Debugf("TiKV store already closed")
		return nil
	}

	err := t.client.Close()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to close TiKV client: %v", err)
		return fmt.Errorf("failed to close TiKV client: %w", err)
	}

	t.closed = true
	logger.GetLogger("boulder").Infof("TiKV store closed successfully")
	return nil
}
