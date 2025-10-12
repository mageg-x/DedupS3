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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	xconf "github.com/mageg-x/dedups3/internal/config"
	"github.com/mageg-x/dedups3/internal/logger"
	"time"
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
	opts.Logger = nil       // 禁用日志
	opts.SyncWrites = false // 已设置，保持异步写入

	// 综合优化配置
	// 🎯 内存驻留优化组合
	opts.MemTableSize = 512 << 20     // 512MB - 大内存表
	opts.NumMemtables = 10            // 10个内存表
	opts.NumLevelZeroTables = 20      // 延迟L0压缩
	opts.NumLevelZeroTablesStall = 40 // 提高停滞阈值

	// 💾 缓存优化
	opts.BlockCacheSize = 1024 << 20 // 1GB块缓存
	opts.IndexCacheSize = 512 << 20  // 512MB索引缓存

	// 📊 LSM优化
	opts.BaseTableSize = 16 << 20  // 16MB基础表
	opts.BaseLevelSize = 256 << 20 // 256MB基础层级
	opts.LevelSizeMultiplier = 20  // 减少压缩频率

	// 🚀 Value存储优化
	opts.ValueThreshold = 1024 // 1KB阈值
	opts.VLogPercentile = 0.99 // 99%动态阈值
	opts.NumCompactors = 2     // 最小化后台压缩

	// ⚡ 性能优化
	opts.Compression = options.ZSTD
	opts.ZSTDCompressionLevel = 1
	opts.VerifyValueChecksum = false // 关闭校验提升性能

	db, err := badger.Open(opts)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to open badgerdb: %v", err)
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	logger.GetLogger("dedups3").Infof("badgerdb store initialized successfully")
	return &BadgerStore{db: db}, nil
}

func (b *BadgerStore) Get(key string, value interface{}) (bool, error) {
	txn, err := b.BeginTxn(context.Background(), &TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize tikv txn: %v", err)
		return false, err
	}
	defer txn.Rollback()
	return txn.Get(key, value)
}

func (b *BadgerStore) GetRaw(key string) ([]byte, bool, error) {
	txn, err := b.BeginTxn(context.Background(), &TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize tikv txn: %v", err)
		return nil, false, err
	}
	defer txn.Rollback()
	return txn.GetRaw(key)
}

func (b *BadgerStore) BatchGet(keys []string) (map[string][]byte, error) {
	txn, err := b.BeginTxn(context.Background(), &TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize tikv txn: %v", err)
		return nil, err
	}
	defer txn.Rollback()
	return txn.BatchGet(keys)
}

func (b *BadgerStore) Set(key string, value interface{}) error {
	txn, err := b.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize tikv txn: %v", err)
		return err
	}
	defer func() {
		if txn != nil {
			txn.Rollback()
		}
	}()
	err = txn.Set(key, value)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed set key %s, %v", key, err)
		return fmt.Errorf("failed set key %s, %w", key, err)
	}
	err = txn.Commit()
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction %s: %v", key, err)
		return fmt.Errorf("failed to commit transaction %s: %w", key, err)
	}
	txn = nil
	logger.GetLogger("dedups3").Debugf("success setting key %s", key)
	return nil
}

func (b *BadgerStore) Incr(k string) (uint64, error) {
	logger.GetLogger("dedups3").Debugf("generating next ID for key: %s", k)

	txn, err := b.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to begin transaction: %v", err)
		return 0, err
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
		return 0, err
	}

	var id uint64
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
		return 0, err
	}

	// 提交事务
	err = txn.Commit()
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return 0, err
	}
	txn = nil

	logger.GetLogger("dedups3").Debugf("setting new ID %d for key: %s", id, k)
	return id, nil
}

func (b *BadgerStore) Delete(key string) error {
	txn, err := b.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize tikv txn: %v", err)
		return err
	}
	defer func() {
		if txn != nil {
			txn.Rollback()
		}
	}()
	err = txn.Delete(key)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete key %s: %v", key, err)
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	err = txn.Commit()
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction for key %s : %v", key, err)
		return fmt.Errorf("failed to commit transaction for key %s: %w", key, err)
	}
	txn = nil
	logger.GetLogger("dedups3").Debugf("success deleted key %s", key)
	return nil
}

// BeginTxn 开始一个新事务
// 根据传入的TxnOpt配置事务类型和行为
func (b *BadgerStore) BeginTxn(_ context.Context, opt *TxnOpt) (Txn, error) {
	// 如果没有提供选项或IsReadOnly为false，则使用默认的读写事务
	update := true
	if opt != nil && opt.IsReadOnly {
		update = false
	}

	txn := b.db.NewTransaction(update)
	return &BadgerTxn{txn: txn}, nil
}

// TryLock 尝试获取锁，成功返回true，失败返回false
func (b *BadgerStore) TryLock(key, owner string, ttl time.Duration) (bool, error) {
	// 锁的实际键名，添加前缀避免冲突
	lockKey := key

	txn, err := b.BeginTxn(context.Background(), nil)
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
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
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

// UnLock  释放锁
func (b *BadgerStore) UnLock(key, owner string) error {
	// 锁的实际键名，添加前缀避免冲突
	lockKey := key

	txn, err := b.BeginTxn(context.Background(), nil)
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
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
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
		} else {
			logger.GetLogger("dedups3").Debugf("deleted lock %s", lockKey)
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

// Close 关闭数据库
func (b *BadgerStore) Close() error {
	if b.db == nil {
		logger.GetLogger("dedups3").Errorf("database already closed")
		return errors.New("database already closed")
	}

	if err := b.db.Close(); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to close badgerdb: %v", err)
		return fmt.Errorf("failed to close badger db: %w", err)
	}

	logger.GetLogger("dedups3").Debugf("badgerdb store closed successfully")
	return nil
}

// Get 在事务中获取值
func (t *BadgerTxn) Get(key string, value interface{}) (bool, error) {
	data, exists, err := t.GetRaw(key)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("error getting key %s: %v", key, err)
		return false, err
	}
	if !exists {
		logger.GetLogger("dedups3").Debugf("key not found: %s", key)
		return false, nil
	}

	if err := json.Unmarshal(data, value); err != nil {
		logger.GetLogger("dedups3").Errorf("json unmarshal error for key %s: %v", key, err)
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	//logger.GetLogger("dedups3").Debugf("successfully got key: %s, data len %d", key, len(data))
	return true, nil
}

// GetRaw 在事务中获取原始字节数据
func (t *BadgerTxn) GetRaw(key string) ([]byte, bool, error) {
	var data []byte
	item, err := t.txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			logger.GetLogger("dedups3").Debugf("key not found: %s", key)
			return nil, false, nil
		}
		return nil, false, err
	}

	err = item.Value(func(val []byte) error {
		data = make([]byte, len(val))
		copy(data, val)
		return nil
	})
	if err != nil {
		logger.GetLogger("dedups3").Debugf("failed got raw data for key: %s", key)
		return nil, false, err
	}
	//logger.GetLogger("dedups3").Debugf("successfully get key %s data %d", key, len(data))
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
		logger.GetLogger("dedups3").Errorf("json marshal error for key %s: %v", key, err)
		return fmt.Errorf("json marshal error: %w", err)
	}
	err = t.txn.Set([]byte(key), data)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to put key %s: %v", key, err)
	}
	logger.GetLogger("dedups3").Debugf("successfully set key %s data %d", key, len(data))
	return err
}

func (t *BadgerTxn) SetNX(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("json marshal error for key %s: %v", key, err)
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
			logger.GetLogger("dedups3").Errorf("json marshal error for key %s: %v", key, err)
			return fmt.Errorf("json marshal error for key %s: %w", key, err)
		}

		// 设置键值对
		err = t.txn.Set([]byte(key), data)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to set key %s: %v", key, err)
			return fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return nil
}

func (t *BadgerTxn) Delete(key string) error {
	err := t.txn.Delete([]byte(key))
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete key %s: %v", key, err)
	} else {
		//utils.DumpCaller(10)
		logger.GetLogger("dedups3").Debugf("success delete key %s", key)
	}
	return err
}
func (t *BadgerTxn) DeletePrefix(prefix string, limit int32) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte(prefix)

	it := t.txn.NewIterator(opts)
	defer it.Close()

	var count int32
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()

		// 删除当前 key
		if err := t.txn.Delete(key); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to delete key %s: %v", string(key), err)
			return fmt.Errorf("failed to delete key %s: %w", string(key), err)
		}

		count++

		// 如果设置了 limit 且已达到数量，退出
		if limit > 0 && count >= limit {
			break
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
		//logger.GetLogger("dedups3").Errorf("found key %s in prefix %s", string(key), string(seekKey))
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
