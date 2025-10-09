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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	pd "github.com/tikv/pd/client"
	"time"

	xconf "github.com/mageg-x/dedups3/internal/config"
	"github.com/mageg-x/dedups3/internal/logger"
)

// TiKVStore 基于TiKV的KV存储实现
type TiKVStore struct {
	client *tikv.KVStore
}

// TiKVTxn 基于TiKV的事务实现
type TiKVTxn struct {
	txn *tikv.KVTxn
}

// TiKVReadOnlyTxn 只读事务包装器
type TiKVReadOnlyTxn struct {
	snapshot *txnsnapshot.KVSnapshot
}

// InitTiKVStore 初始化TiKV存储
func InitTiKVStore(cfg *xconf.TiKVConfig) (*TiKVStore, error) {
	// 创建安全配置
	security := pd.SecurityOption{}

	pdCli, err := pd.NewClient(cfg.PDAddrs, security)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize TiKV pd client: %v", err)
		return nil, err
	}
	client, err := tikv.NewKVStore("tikv", pdCli, nil, nil)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize TiKV  client: %v", err)
		return nil, err
	}
	return &TiKVStore{client: client}, nil
}

// Get 非事务版
func (t *TiKVStore) Get(key string, value interface{}) (bool, error) {
	// 直接使用 TiKV 快照 Get 方法进行快照读取
	snapshot := t.client.GetSnapshot(0)
	if snapshot == nil {
		logger.GetLogger("dedups3").Errorf("failed to get snapshot for key: %s", key)
		return false, fmt.Errorf("failed to get snapshot for key: %s", key)
	}

	data, err := snapshot.Get(context.Background(), []byte(key))
	if err != nil {
		if errors.Is(err, tikverr.ErrNotExist) {
			return false, nil
		}
		logger.GetLogger("dedups3").Errorf("Error getting key %s: %v", key, err)
		return false, err
	}

	if data == nil {
		logger.GetLogger("dedups3").Debugf("Key not found: %s", key)
		return false, nil
	}

	if err := json.Unmarshal(data, value); err != nil {
		logger.GetLogger("dedups3").Errorf("JSON unmarshal error for key %s: %v", key, err)
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	logger.GetLogger("dedups3").Debugf("Successfully got value for key: %s", key)
	return true, nil
}

func (t *TiKVStore) GetRaw(key string) ([]byte, bool, error) {
	// 直接使用 TiKV 快照 Get 方法进行快照读取
	snapshot := t.client.GetSnapshot(0)
	if snapshot == nil {
		logger.GetLogger("dedups3").Errorf("failed to get snapshot for key: %s", key)
		return nil, false, fmt.Errorf("failed to get snapshot for key: %s", key)
	}

	data, err := snapshot.Get(context.Background(), []byte(key))
	if err != nil {
		if errors.Is(err, tikverr.ErrNotExist) {
			return nil, false, nil
		}
		logger.GetLogger("dedups3").Errorf("Error getting key %s: %v", key, err)
		return nil, false, err
	}

	return data, true, nil
}

func (t *TiKVStore) BatchGet(keys []string) (map[string][]byte, error) {
	// 直接使用 TiKV 快照 Get 方法进行快照读取
	snapshot := t.client.GetSnapshot(0)
	if snapshot == nil {
		logger.GetLogger("dedups3").Errorf("failed to get snapshot for  batch get key")
		return nil, fmt.Errorf("failed to get snapshot for batch get key")
	}
	ks := make([][]byte, 0, len(keys))
	for _, key := range keys {
		ks = append(ks, []byte(key))
	}
	values, err := snapshot.BatchGet(context.Background(), ks)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to BatchGet keys: %v", err)
		return nil, fmt.Errorf("failed to BatchGet keys: %w", err)
	}

	return values, nil
}

func (t *TiKVStore) Set(key string, value interface{}) error {
	txn, err := t.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to begin transaction: %v", err)
		return err
	}
	defer func() {
		if txn != nil {
			txn.Rollback()
		}
	}()

	err = txn.Set(key, value)
	if err == nil {
		logger.GetLogger("dedups3").Infof("Successfully set value for key: %s", key)
		err = txn.Commit()
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		}
		txn = nil
	}
	return err
}

// Incr 原子性地获取并递增ID值
func (t *TiKVStore) Incr(k string) (uint64, error) {
	var id uint64

	logger.GetLogger("dedups3").Debugf("generating next ID for key: %s", k)

	txn, err := t.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to begin transaction for key %s : %v", k, err)
		return id, fmt.Errorf("failed to begin transaction for key %s: %w", k, err)
	}
	defer func() {
		if txn != nil {
			txn.Rollback()
		}
	}()

	// 尝试获取当前值
	rawData, exists, err := txn.GetRaw(k)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get key %s: %v", k, err)
		return id, fmt.Errorf("failed to get key %s: %w", k, err)
	}

	// 计算新ID
	if !exists {
		id = 1
		logger.GetLogger("dedups3").Debugf("key %s not found, initializing to 1", k)
	} else {
		id = binary.LittleEndian.Uint64(rawData) + 1
	}

	// 保存新ID
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], id)
	err = txn.Set(k, buf[:])
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set key %s: %v", k, err)
		return id, fmt.Errorf("failed to set key %s: %w", k, err)
	}

	// 提交事务
	err = txn.Commit()
	if err != nil {
		logger.GetLogger("dedups3").Warnf("transaction conflict for key %s , %w", k, err)
		return id, fmt.Errorf("transaction conflict for key %s , %w", k, err)
	}
	txn = nil

	// 成功获取新ID
	logger.GetLogger("dedups3").Debugf("setting new ID %d for key: %s", id, k)
	return id, nil
}

func (t *TiKVStore) Delete(key string) error {
	txn, err := t.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to begin transaction: %v", err)
		return err
	}
	defer func() {
		if txn != nil {
			txn.Rollback()
		}
	}()
	err = txn.Delete(key)
	if err == nil {
		logger.GetLogger("dedups3").Infof("Successfully delete key: %s", key)
		err = txn.Commit()
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		}
		txn = nil
	}
	return err
}

// BeginTxn 开始一个新事务
func (t *TiKVStore) BeginTxn(_ context.Context, opt *TxnOpt) (Txn, error) {
	txn, err := t.client.Begin()
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to begin txn: %v", err)
		return nil, err
	}

	// 对于只读事务，我们通常设置低优先级
	if opt != nil && opt.IsReadOnly {
		// 只读事务使用快照
		snapshot := t.client.GetSnapshot(0) // 0 表示最新版本
		// 创建只读事务包装器
		return &TiKVReadOnlyTxn{snapshot: snapshot}, nil
	} else {
		return &TiKVTxn{txn: txn}, nil
	}
}

// TryLock 尝试获取锁，成功返回true，失败返回false
func (t *TiKVStore) TryLock(key, owner string, ttl time.Duration) (bool, error) {
	// 锁的实际键名，添加前缀避免冲突
	lockKey := "lock:" + key

	txn, err := t.BeginTxn(context.Background(), nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if txn != nil {
			txn.Rollback()
		}
	}()

	// 检查锁是否存在且未过期
	preVal := LockVal{}
	exists, err := txn.Get(lockKey, &preVal)
	if err != nil && !errors.Is(err, tikverr.ErrNotExist) {
		return false, fmt.Errorf("failed to check lock: %w", err)
	}

	// 锁存在且未过期，获取失败
	if exists && time.Now().Before(preVal.ExpiresAt) {
		return false, nil
	}

	// 尝试设置锁
	lockVal := LockVal{Owner: owner, ExpiresAt: time.Now().Add(ttl)}
	err = txn.Set(lockKey, &lockVal)
	if err != nil {
		return false, fmt.Errorf("failed to set lock: %w", err)
	}

	// 提交事务
	err = txn.Commit()
	if err != nil {
		return false, fmt.Errorf("failed to commit lock transaction: %w", err)
	}
	txn = nil
	return true, nil
}

// Lock 获取锁，失败时会重试，直到成功或超时
func (t *TiKVStore) UnLock(key, owner string) error {
	// 锁的实际键名，添加前缀避免冲突
	lockKey := "lock:" + key

	txn, err := t.BeginTxn(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if txn != nil {
			txn.Rollback()
		}
	}()

	// 检查锁是否存在
	preLockVal := LockVal{}
	exists, err := txn.Get(key, &preLockVal)
	if err != nil && !errors.Is(err, tikverr.ErrNotExist) {
		return fmt.Errorf("failed to check lock: %w", err)
	}

	// 如果锁不存在，直接返回成功
	if !exists {
		return nil
	}

	// 检查是否是自己持有的锁
	if preLockVal.Owner == owner {
		// 是自己的锁，删除
		err = txn.Delete(lockKey)
		if err != nil {
			return fmt.Errorf("failed to delete lock: %w", err)
		}
		// 提交事务
		err = txn.Commit()
		if err != nil {
			return fmt.Errorf("failed to commit unlock transaction: %w", err)
		}
		txn = nil
	}
	// 不是自己的锁，不能释放
	return fmt.Errorf("cannot unlock: not the lock owner")
}

// Close 关闭连接
func (t *TiKVStore) Close() error {
	err := t.client.Close()
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to close TiKV client: %v", err)
		return fmt.Errorf("failed to close TiKV client: %w", err)
	}

	logger.GetLogger("dedups3").Debugf("TiKV store closed successfully")
	return nil
}

func (t *TiKVTxn) Commit() error {
	err := t.txn.Commit(context.Background())
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit txn: %v", err)
		return fmt.Errorf("failed to commit txn: %w", err)
	}
	logger.GetLogger("dedups3").Debugf("Committed txn")
	return nil
}

func (t *TiKVTxn) Rollback() error {
	err := t.txn.Rollback()
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to rollback txn: %v", err)
		return fmt.Errorf("failed to rollback txn: %w", err)
	}
	logger.GetLogger("dedups3").Debugf("Rolled back txn")
	return nil
}

func (t *TiKVTxn) Get(key string, value interface{}) (bool, error) {
	data, err := t.txn.Get(context.Background(), []byte(key))
	if err != nil {
		if errors.Is(err, tikverr.ErrNotExist) {
			return false, nil
		}
		logger.GetLogger("dedups3").Errorf("Error getting key %s: %v", key, err)
		return false, err
	}

	if data == nil {
		logger.GetLogger("dedups3").Debugf("Key not found: %s", key)
		return false, nil
	}

	if err := json.Unmarshal(data, value); err != nil {
		logger.GetLogger("dedups3").Errorf("JSON unmarshal error for key %s: %v", key, err)
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	logger.GetLogger("dedups3").Debugf("Successfully got value for key: %s", key)
	return true, nil
}

func (t *TiKVTxn) GetRaw(key string) ([]byte, bool, error) {
	data, err := t.txn.Get(context.Background(), []byte(key))
	if err != nil {
		if errors.Is(err, tikverr.ErrNotExist) {
			return nil, false, nil
		}
		logger.GetLogger("dedups3").Errorf("Error getting raw data for key %s: %v", key, err)
		return nil, false, err
	}

	if data == nil {
		logger.GetLogger("dedups3").Debugf("Key not found: %s", key)
		return nil, false, nil
	}
	logger.GetLogger("dedups3").Debugf("Successfully got raw data for key: %s", key)
	return data, true, nil
}

func (t *TiKVTxn) BatchGet(keys []string) (map[string][]byte, error) {
	ks := make([][]byte, 0, len(keys))
	for _, key := range keys {
		ks = append(ks, []byte(key))
	}
	values, err := t.txn.BatchGet(context.Background(), ks)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to BatchGet keys: %v", err)
		return nil, fmt.Errorf("failed to BatchGet keys: %w", err)
	}

	return values, nil
}

func (t *TiKVTxn) Set(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("JSON marshal error for key %s: %v", key, err)
		return fmt.Errorf("json marshal error: %w", err)
	}

	err = t.txn.Set([]byte(key), data)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to put key %s: %v", key, err)
	}
	return err
}

func (t *TiKVTxn) SetNX(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("JSON marshal error for key %s: %v", key, err)
		return fmt.Errorf("json marshal error: %w", err)
	}
	// 检查key是否存在
	_, err = t.txn.Get(context.Background(), []byte(key))
	if err == nil {
		logger.GetLogger("dedups3").Errorf("Error getting key %s: %v", key, err)
		return ErrKeyExists
	}

	// key不存在，设置值
	err = t.txn.Set([]byte(key), data)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to put key %s: %v", key, err)
		return err
	}
	return nil
}

func (t *TiKVTxn) BatchSet(kvs map[string]interface{}) error {
	// 遍历所有键值对
	for key, value := range kvs {
		// 序列化值
		data, err := json.Marshal(value)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("JSON marshal error for key %s: %v", key, err)
			return fmt.Errorf("json marshal error for key %s: %w", key, err)
		}

		// 设置键值对
		err = t.txn.Set([]byte(key), data)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("Failed to set key %s: %v", key, err)
			return fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return nil
}

func (t *TiKVTxn) Delete(key string) error {
	err := t.txn.Delete([]byte(key))
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to delete key %s: %v", key, err)
	} else {
		logger.GetLogger("dedups3").Debugf("Successfully deleted key: %s", key)
	}
	return err
}

func (t *TiKVTxn) DeletePrefix(prefix string, limit int32) error {
	// 计算结束前缀：例如 user: -> user; （字节序上刚好大于 user: 的最小前缀）
	endPrefix := make([]byte, len(prefix))
	copy(endPrefix, prefix)
	for i := len(endPrefix) - 1; i >= 0; i-- {
		if endPrefix[i] < 0xFF {
			endPrefix[i]++
			endPrefix = endPrefix[:i+1]
			break
		}
		// 如果字节是0xFF，继续向前寻找可以递增的字节
		if i == 0 {
			// 整个前缀都是0xFF，需要特殊处理
			endPrefix = append([]byte(prefix), 0x00)
		}
	}

	// 创建迭代器
	iter, err := t.txn.Iter([]byte(prefix), endPrefix)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to create iterator for prefix %s: %v", prefix, err)
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var count int32
	// 遍历所有匹配前缀的键并删除
	for iter.Valid() && (limit == 0 || count < limit) {
		key := iter.Key()
		count += 1
		// 删除键
		err := t.txn.Delete(key)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("Failed to delete key %s: %v", string(key), err)
			return fmt.Errorf("failed to delete key %s: %w", string(key), err)
		}

		err = iter.Next()
		if err != nil {
			logger.GetLogger("dedups3").Errorf("Failed to advance iterator: %v", err)
			return fmt.Errorf("failed to advance iterator: %w", err)
		}
	}

	return nil
}

func (t *TiKVTxn) Scan(prefix string, startKey string, limit int) ([]string, string, error) {
	// 确定起始键
	start := []byte(startKey)
	if startKey == "" {
		start = []byte(prefix)
	}
	// 计算结束前缀：例如 user: -> user; （字节序上刚好大于 user: 的最小前缀）
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			end = end[:i+1]
			break
		}
		// 如果字节是0xFF，继续向前寻找可以递增的字节
		if i == 0 {
			// 整个前缀都是0xFF，需要特殊处理
			end = append([]byte(prefix), 0x00)
		}
	}

	// 执行扫描
	// 创建迭代器
	iter, err := t.txn.Iter(start, end)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to create iterator for prefix %s: %v", prefix, err)
		return nil, "", fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	keys := make([]string, 0)
	for iter.Valid() && len(keys) < limit {
		key := iter.Key()
		keys = append(keys, string(key))
	}

	// 确定下一个起始键
	var nextKey string
	if len(keys) == limit && len(keys) > 0 {
		lastKey := keys[len(keys)-1]
		nextKey = lastKey
	}
	return keys, nextKey, nil
}

// Get 在只读事务中获取值
func (t *TiKVReadOnlyTxn) Get(key string, value interface{}) (bool, error) {
	data, exists, err := t.GetRaw(key)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	if err := json.Unmarshal(data, value); err != nil {
		logger.GetLogger("dedups3").Errorf("JSON unmarshal error for key %s: %v", key, err)
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	return true, nil
}

// GetRaw 在只读事务中获取原始字节数据
func (t *TiKVReadOnlyTxn) GetRaw(key string) ([]byte, bool, error) {
	data, err := t.snapshot.Get(context.Background(), []byte(key))
	if err != nil {
		if errors.Is(err, tikverr.ErrNotExist) {
			return nil, false, nil
		}
		logger.GetLogger("dedups3").Errorf("Error getting raw data for key %s: %v", key, err)
		return nil, false, err
	}

	if data == nil {
		return nil, false, nil
	}
	return data, true, nil
}

// BatchGet 在只读事务中批量获取值
func (t *TiKVReadOnlyTxn) BatchGet(keys []string) (map[string][]byte, error) {
	ks := make([][]byte, 0, len(keys))
	for _, key := range keys {
		ks = append(ks, []byte(key))
	}

	// 直接使用快照的批量获取方法
	values, err := t.snapshot.BatchGet(context.Background(), ks)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to BatchGet keys: %v", err)
		return nil, fmt.Errorf("failed to BatchGet keys: %w", err)
	}

	return values, nil
}

// Scan 在只读事务中扫描键
func (t *TiKVReadOnlyTxn) Scan(prefix string, startKey string, limit int) ([]string, string, error) {
	// 确定起始键
	start := []byte(startKey)
	if startKey == "" {
		start = []byte(prefix)
	}
	// 计算结束前缀
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			end = end[:i+1]
			break
		}
		// 如果字节是0xFF，继续向前寻找可以递增的字节
		if i == 0 {
			// 整个前缀都是0xFF，需要特殊处理
			end = append([]byte(prefix), 0x00)
		}
	}

	// 执行扫描
	iter, err := t.snapshot.Iter(start, end)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to create iterator for prefix %s: %v", prefix, err)
		return nil, "", fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	keys := make([]string, 0)
	for iter.Valid() && len(keys) < limit {
		key := iter.Key()
		keys = append(keys, string(key))
		iter.Next() // 移动到下一个键
	}

	// 确定下一个起始键
	var nextKey string
	if len(keys) == limit && len(keys) > 0 {
		lastKey := keys[len(keys)-1]
		nextKey = lastKey
	}
	return keys, nextKey, nil
}

// Set 只读事务不支持写操作，返回错误
func (t *TiKVReadOnlyTxn) Set(key string, value interface{}) error {
	return fmt.Errorf("write operations not supported in read-only transaction")
}

// SetNX 只读事务不支持写操作，返回错误
func (t *TiKVReadOnlyTxn) SetNX(key string, value interface{}) error {
	return fmt.Errorf("write operations not supported in read-only transaction")
}

// BatchSet 只读事务不支持写操作，返回错误
func (t *TiKVReadOnlyTxn) BatchSet(kvs map[string]interface{}) error {
	return fmt.Errorf("write operations not supported in read-only transaction")
}

// Delete 只读事务不支持写操作，返回错误
func (t *TiKVReadOnlyTxn) Delete(key string) error {
	return fmt.Errorf("write operations not supported in read-only transaction")
}

// DeletePrefix 只读事务不支持写操作，返回错误
func (t *TiKVReadOnlyTxn) DeletePrefix(prefix string, limit int32) error {
	return fmt.Errorf("write operations not supported in read-only transaction")
}

// Commit 只读事务不需要提交，直接返回nil
func (t *TiKVReadOnlyTxn) Commit() error {
	return nil
}

// Rollback 只读事务不需要回滚，直接返回nil
func (t *TiKVReadOnlyTxn) Rollback() error {
	return nil
}
