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
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormloger "gorm.io/gorm/logger"

	"github.com/mageg-x/dedups3/internal/logger"
)

// KVData GORM模型结构，包含cluster、namespace和version字段
type KVData struct {
	Cluster   string    `gorm:"column:cluster;index"`
	Namespace string    `gorm:"column:namespace;index"`
	Key       string    `gorm:"primaryKey;column:key"`
	Value     string    `gorm:"column:value"`
	Version   int64     `gorm:"column:version;default:0"` // 用于CAS操作的版本号
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TransactionSession 表示一个事务会话，包含GORM事务和创建时间
type TransactionSession struct {
	Tx      *gorm.DB
	Created time.Time
}

// SQLiteClient 实现 kvconfig 接口
type SQLiteClient struct {
	db   *gorm.DB
	mu   sync.RWMutex
	txns map[string]*TransactionSession // 活跃事务会话，存储gorm事务实例和创建时间
	quit chan struct{}                  // 用于停止清理协程
	wg   sync.WaitGroup                 // 用于等待清理协程结束

	// 缓存相关字段
	cache     map[string]string // 缓存原始JSON数据，key是Get方法传入的key
	cacheMu   sync.RWMutex      // 保护缓存的读写锁
	cacheSize int               // 缓存最大容量
}

// NewSQLiteClient 创建新的 SQLite 客户端
func NewSQLiteClient() *SQLiteClient {
	client := &SQLiteClient{
		txns:      make(map[string]*TransactionSession),
		quit:      make(chan struct{}),
		cache:     make(map[string]string),
		cacheSize: 10000, // 默认缓存10000个项目
	}

	// 启动事务清理协程
	client.wg.Add(1)
	go client.cleanupExpiredTransactions()

	return client
}

func (s *SQLiteClient) Open(args *Args) error {
	if args.Driver != "sqlite" {
		return fmt.Errorf("driver must be 'sqlite'")
	}
	if abspath, err := filepath.Abs(args.DSN); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid dsn %s", args.DSN)
		return fmt.Errorf("invalid dsn %s", args.DSN)
	} else {
		args.DSN = abspath
		dir := path.Dir(args.DSN)
		if err = os.MkdirAll(dir, 0755); err != nil {
			logger.GetLogger("dedups3").Errorf("failed permission to create dir %s", args.DSN)
			return fmt.Errorf("failed permission to create dir %s", args.DSN)
		}
	}

	db, err := gorm.Open(sqlite.Open(args.DSN), &gorm.Config{
		Logger: gormloger.Default.LogMode(gormloger.Silent), // 完全关闭 GORM 内部日志
	})
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to open database %s", args.DSN)
		return err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return err
	}

	// 设置 SQLite 连接参数
	sqlDB.SetMaxOpenConns(10) // SQLite 建议单连接
	s.db = db

	return nil
}

func (s *SQLiteClient) Close() error {
	// 停止清理协程
	close(s.quit)
	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	// 回滚所有活跃事务
	for sessionID := range s.txns {
		// 在GORM中，未提交的事务会在连接关闭时自动回滚
		delete(s.txns, sessionID)
	}

	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}

	return sqlDB.Close()
}

// Get 普通数据操作接口
func (s *SQLiteClient) Get(key string, tpl interface{}) (interface{}, error) {
	if tpl == nil {
		return nil, fmt.Errorf("no tpl provided")
	}
	tplType := reflect.TypeOf(tpl)
	if tplType.Kind() == reflect.Ptr {
		return nil, fmt.Errorf("tpl must be non-pointer type, got %s", tplType.Kind())
	}

	appName, _, _, err := s.extractKeyParts(key)
	if err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}
	tableName := appName

	// 尝试从缓存中获取
	cacheKey := fmt.Sprintf("%s:%s", appName, key)
	s.cacheMu.RLock()
	cachedValue, found := s.cache[cacheKey]
	s.cacheMu.RUnlock()

	if found {
		clone := reflect.New(tplType).Interface() // 类型是 *YourType
		if err := json.Unmarshal([]byte(cachedValue), clone); err != nil {
			return nil, fmt.Errorf("failed to unmarshal cached value: %w", err)
		}
		return clone, nil
	}

	// 2. 缓存未命中，从数据库查询
	var kv KVData

	if err := s.db.Table(tableName).Where("key = ?", key).First(&kv).Error; err != nil {
		return nil, err
	}

	// 3. 将查询结果存入缓存
	s.cacheMu.Lock()
	s.cache[cacheKey] = kv.Value
	// 如果缓存超过容量，需要清理
	if len(s.cache) > s.cacheSize {
		s.trimCache()
	}
	s.cacheMu.Unlock()

	clone := reflect.New(tplType).Interface() // 类型是 *YourType
	if err := json.Unmarshal([]byte(kv.Value), clone); err != nil {
		return nil, fmt.Errorf("failed to unmarshal value: %w", err)
	}
	return clone, nil
}

func (s *SQLiteClient) Set(key string, value interface{}) error {
	logger.GetLogger("dedups3").Errorf("set key %s value  %#v", key, value)
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return err
	}

	appName, cluster, namespace, err := s.extractKeyParts(key)
	if err != nil {
		return fmt.Errorf("invalid key: %v", err)
	}
	tableName := appName

	// 确保表存在
	if err := s.createTableForApp(appName); err != nil {
		return err
	}

	// 查询当前记录以获取版本号
	var currentKV KVData
	var version int64 = 0
	if err := s.db.Table(tableName).Where("key = ?", key).First(&currentKV).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		// 记录不存在，版本号为0
	} else {
		// 版本号递增
		version = currentKV.Version + 1
	}

	kv := KVData{
		Key:       key,
		Value:     string(valueJSON),
		Cluster:   cluster,
		Namespace: namespace,
		Version:   version,
		UpdatedAt: time.Now().UTC(),
	}

	// 使用Clauses(clause.OnConflict{DoUpdate: ...})实现INSERT OR REPLACE
	result := s.db.Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value", "cluster", "namespace", "version", "updated_at"}),
	}).Create(&kv)

	if result.Error == nil {
		// 更新缓存
		cacheKey := fmt.Sprintf("%s:%s", appName, key)
		s.cacheMu.Lock()
		s.cache[cacheKey] = string(valueJSON)
		s.cacheMu.Unlock()
	}

	return result.Error
}

func (s *SQLiteClient) Del(key string) error {
	appName, _, _, err := s.extractKeyParts(key)
	if err != nil {
		return fmt.Errorf("invalid key: %v", err)
	}
	tableName := appName

	result := s.db.Table(tableName).Delete(&KVData{}, "key = ?", key)
	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("key not found")
	}

	// 从缓存中删除
	cacheKey := fmt.Sprintf("%s:%s", appName, key)
	s.cacheMu.Lock()
	if _, exists := s.cache[cacheKey]; exists {
		delete(s.cache, cacheKey)
	}
	s.cacheMu.Unlock()

	return nil
}

func (s *SQLiteClient) Create(key string, value interface{}) error {
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return err
	}

	appName, cluster, namespace, err := s.extractKeyParts(key)
	if err != nil {
		return fmt.Errorf("invalid key: %v", err)
	}
	tableName := appName

	// 确保表存在
	if err := s.createTableForApp(appName); err != nil {
		return err
	}

	kv := KVData{
		Key:       key,
		Value:     string(valueJSON),
		Cluster:   cluster,
		Namespace: namespace,
		Version:   1, // 新创建的记录版本号为1
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	result := s.db.Table(tableName).Create(&kv)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrDuplicatedKey) {
			return fmt.Errorf("key already exists")
		}
		return fmt.Errorf("insert failed: %w", result.Error)
	}

	// 添加到缓存
	cacheKey := fmt.Sprintf("%s:%s", appName, key)
	s.cacheMu.Lock()
	s.cache[cacheKey] = string(valueJSON)
	if len(s.cache) > s.cacheSize {
		s.trimCache()
	}
	s.cacheMu.Unlock()

	return nil
}

func (s *SQLiteClient) List(prefix, marker string, limit int, tpl interface{}) (map[string]interface{}, string, error) {
	if tpl == nil {
		return nil, "", fmt.Errorf("no tpl provided")
	}
	tplType := reflect.TypeOf(tpl)
	if tplType.Kind() == reflect.Ptr {
		return nil, "", fmt.Errorf("tpl must be non-pointer type, got %s", tplType.Kind())
	}

	// 从prefix中提取app名称来确定要查询的表
	var appName string
	if prefix != "" {
		var err error
		appName, _, _, err = s.extractKeyParts(prefix)
		if err != nil {
			return nil, "", fmt.Errorf("invalid prefix: %w", err)
		}
	} else if marker != "" {
		var err error
		appName, _, _, err = s.extractKeyParts(marker)
		if err != nil {
			return nil, "", fmt.Errorf("invalid marker: %w", err)
		}
	} else {
		// 如果没有prefix和marker，无法确定要查询哪个表，返回默认app
		appName = "default"
	}

	tableName := appName

	// 确保表存在
	if err := s.createTableForApp(appName); err != nil {
		return nil, "", err
	}

	var kvs []KVData
	query := s.db.Table(tableName).Model(&KVData{})

	if prefix != "" {
		query = query.Where("key LIKE ?", prefix+"%")
	}

	if marker != "" {
		query = query.Where("key > ?", marker)
	}

	query = query.Order("key")

	if limit > 0 {
		query = query.Limit(limit + 1) // 多取一个用来判断是否有下一页
	}

	if err := query.Find(&kvs).Error; err != nil {
		return nil, "", err
	}

	result := make(map[string]interface{})
	var lastKey string
	count := 0

	for _, kv := range kvs {
		count++
		if limit > 0 && count > limit {
			lastKey = kv.Key
			break
		}

		clone := reflect.New(tplType).Interface() // 类型是 *YourType
		if err := json.Unmarshal([]byte(kv.Value), clone); err != nil {
			// 解析失败时
			return nil, "", fmt.Errorf("failed to unmarshal value: %v", err)
		} else {
			result[kv.Key] = clone
		}
	}

	return result, lastKey, nil
}

// TxnBegin 事务数据操作接口
func (s *SQLiteClient) TxnBegin() (string, error) {
	tx := s.db.Begin()
	if tx.Error != nil {
		return "", tx.Error
	}

	sessionID := fmt.Sprintf("sqlite_sess_%d", time.Now().UTC().UnixNano())

	s.mu.Lock()
	s.txns[sessionID] = &TransactionSession{
		Tx:      tx,
		Created: time.Now().UTC(),
	}
	s.mu.Unlock()

	return sessionID, nil
}

func (s *SQLiteClient) TxnCommit(sessionID string) error {
	session, err := s.getTransaction(sessionID)
	if err != nil {
		return err
	}

	err = session.Tx.Commit().Error
	if err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.txns, sessionID)
	s.mu.Unlock()

	return nil
}

func (s *SQLiteClient) TxnRollback(sessionID string) error {
	session, err := s.getTransaction(sessionID)
	if err != nil {
		return err
	}

	err = session.Tx.Rollback().Error
	if err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.txns, sessionID)
	s.mu.Unlock()

	return nil
}

func (s *SQLiteClient) TxnGetKv(sessionID string, key string, tpl interface{}) (interface{}, error) {
	if tpl == nil {
		return nil, fmt.Errorf("no tpl provided")
	}
	tplType := reflect.TypeOf(tpl)
	if tplType.Kind() == reflect.Ptr {
		return nil, fmt.Errorf("tpl must be non-pointer type, got %s", tplType.Kind())
	}

	appName, _, _, err := s.extractKeyParts(key)
	if err != nil {
		return nil, fmt.Errorf("invalid key: %v", err)
	}
	tableName := appName

	// 确保表存在
	if err := s.createTableForApp(appName); err != nil {
		return nil, err
	}

	session, err := s.getTransaction(sessionID)
	if err != nil {
		return nil, err
	}

	var kv KVData
	if err := session.Tx.Table(tableName).Where("key = ?", key).First(&kv).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("key not found %w", err)
		}
		return nil, err
	}

	// 创建 tplType 的新实例指针
	clone := reflect.New(tplType).Interface() // 类型是 *YourType
	if err := json.Unmarshal([]byte(kv.Value), clone); err != nil {
		return nil, fmt.Errorf("failed to unmarshal value: %w", err)
	}
	return clone, nil
}

func (s *SQLiteClient) TxnSetKv(sessionID string, key string, value interface{}) error {
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return err
	}

	appName, cluster, namespace, err := s.extractKeyParts(key)
	if err != nil {
		return fmt.Errorf("invalid key: %v", err)
	}
	tableName := appName

	// 确保表存在
	if err := s.createTableForApp(appName); err != nil {
		return err
	}

	session, err := s.getTransaction(sessionID)
	if err != nil {
		return err
	}

	// 查询当前记录以获取版本号
	var currentKV KVData
	var version int64 = 0
	if err := session.Tx.Table(tableName).Where("key = ?", key).First(&currentKV).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		// 记录不存在，版本号为0
	} else {
		// 版本号递增
		version = currentKV.Version + 1
	}

	kv := KVData{
		Key:       key,
		Value:     string(valueJSON),
		Cluster:   cluster,
		Namespace: namespace,
		Version:   version,
		UpdatedAt: time.Now().UTC(),
	}

	result := session.Tx.Table(tableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value", "cluster", "namespace", "version", "updated_at"}),
	}).Create(&kv)

	if result.Error == nil {
		// 事务中的修改也需要更新或移除缓存，以避免脏读
		// 直接从缓存中删除，下次查询会重新加载
		cacheKey := fmt.Sprintf("%s:%s", appName, key)
		s.cacheMu.Lock()
		if _, exists := s.cache[cacheKey]; exists {
			delete(s.cache, cacheKey)
		}
		s.cacheMu.Unlock()
	}

	return result.Error
}

func (s *SQLiteClient) TxnDelKv(sessionID string, key string) error {
	session, err := s.getTransaction(sessionID)
	if err != nil {
		return err
	}

	appName, _, _, err := s.extractKeyParts(key)
	if err != nil {
		return fmt.Errorf("invalid key: %v", err)
	}
	tableName := appName

	result := session.Tx.Table(tableName).Delete(&KVData{}, "key = ?", key)
	if result.Error == nil && result.RowsAffected > 0 {
		// 事务中的删除也需要从缓存中删除
		cacheKey := fmt.Sprintf("%s:%s", appName, key)
		s.cacheMu.Lock()
		if _, exists := s.cache[cacheKey]; exists {
			delete(s.cache, cacheKey)
		}
		s.cacheMu.Unlock()
	}

	return result.Error
}

func (s *SQLiteClient) TxnCreateKv(sessionID string, key string, value interface{}) error {
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return err
	}

	appName, cluster, namespace, err := s.extractKeyParts(key)
	if err != nil {
		return fmt.Errorf("invalid key: %v", err)
	}
	tableName := appName

	// 确保表存在
	if err := s.createTableForApp(appName); err != nil {
		return err
	}

	session, err := s.getTransaction(sessionID)
	if err != nil {
		return err
	}
	kv := KVData{
		Key:       key,
		Value:     string(valueJSON),
		Cluster:   cluster,
		Namespace: namespace,
		Version:   1, // 新创建的记录版本号为1
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	result := session.Tx.Table(tableName).Create(&kv)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrDuplicatedKey) {
			return fmt.Errorf("key already exists")
		}
		return fmt.Errorf("insert failed: %w", result.Error)
	}

	// 事务中的创建也需要更新或移除缓存
	// 直接从缓存中删除，下次查询会重新加载
	cacheKey := fmt.Sprintf("%s:%s", appName, key)
	s.cacheMu.Lock()
	if _, exists := s.cache[cacheKey]; exists {
		delete(s.cache, cacheKey)
	}
	s.cacheMu.Unlock()

	return nil
}

func (s *SQLiteClient) TxnListKv(sessionID string, prefix, marker string, limit int, tpl interface{}) (map[string]interface{}, string, error) {
	if tpl == nil {
		return nil, "", fmt.Errorf("no tpl provided")
	}
	tplType := reflect.TypeOf(tpl)
	if tplType.Kind() == reflect.Ptr {
		return nil, "", fmt.Errorf("tpl must be non-pointer type, got %s", tplType.Kind())
	}

	// 从prefix中提取app名称来确定要查询的表
	var appName string
	if prefix != "" {
		var err error
		appName, _, _, err = s.extractKeyParts(prefix)
		if err != nil {
			return nil, "", fmt.Errorf("invalid prefix: %w", err)
		}
	} else if marker != "" {
		var err error
		appName, _, _, err = s.extractKeyParts(marker)
		if err != nil {
			return nil, "", fmt.Errorf("invalid marker: %w", err)
		}
	} else {
		// 如果没有prefix和marker，使用默认app
		appName = "default"
	}

	tableName := appName

	// 确保表存在
	if err := s.createTableForApp(appName); err != nil {
		return nil, "", err
	}

	session, err := s.getTransaction(sessionID)
	if err != nil {
		return nil, "", err
	}

	var kvs []KVData
	query := session.Tx.Table(tableName).Model(&KVData{})

	if prefix != "" {
		query = query.Where("key LIKE ?", prefix+"%")
	}

	if marker != "" {
		query = query.Where("key > ?", marker)
	}

	query = query.Order("key")

	if limit > 0 {
		query = query.Limit(limit + 1) // 多取一个用来判断是否有下一页
	}

	if err := query.Find(&kvs).Error; err != nil {
		return nil, "", err
	}

	result := make(map[string]interface{})
	var lastKey string
	count := 0

	for _, kv := range kvs {
		count++
		if limit > 0 && count > limit {
			lastKey = kv.Key
			break
		}

		clone := reflect.New(tplType).Interface() // 类型是 *YourType
		if err := json.Unmarshal([]byte(kv.Value), clone); err != nil {
			// 解析失败时
			return nil, "", fmt.Errorf("failed to unmarshal value: %v", err)
		} else {
			result[kv.Key] = clone
		}
	}

	return result, lastKey, nil
}

// 辅助方法
func (s *SQLiteClient) createTableForApp(appName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tableName := appName

	var count int64
	// 使用新连接，绕过被占用的连接池
	err := s.db.Session(&gorm.Session{NewDB: true}).Raw(
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name = ?", tableName,
	).Scan(&count).Error
	if err != nil {
		return err
	}

	if count == 0 {
		db := s.db.Session(&gorm.Session{NewDB: true})
		if err := db.Table(tableName).AutoMigrate(&KVData{}); err != nil {
			return err
		}
		if err := db.Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_cluster_namespace ON %s(cluster, namespace)", tableName, tableName)).Error; err != nil {
			return err
		}
		if err := db.Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_updated ON %s(updated_at)", tableName, tableName)).Error; err != nil {
			return err
		}
	}

	return nil
}

func (s *SQLiteClient) getTransaction(sessionID string) (*TransactionSession, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	session, exists := s.txns[sessionID]
	if !exists {
		return nil, fmt.Errorf("transaction session not found")
	}

	return session, nil
}

// cleanupExpiredTransactions 定期清理过期事务
func (s *SQLiteClient) cleanupExpiredTransactions() {
	defer s.wg.Done()

	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	defer ticker.Stop()

	const transactionTimeout = 5 * time.Minute // 事务超时时间为5分钟

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()

			// 检查是否有过期的事务
			now := time.Now().UTC()
			for sessionID, session := range s.txns {
				if now.Sub(session.Created) > transactionTimeout {
					// 回滚过期的事务
					session.Tx.Rollback()
					delete(s.txns, sessionID)
					fmt.Printf("Cleaned up expired transaction: %s\n", sessionID)
				}
			}

			s.mu.Unlock()
		case <-s.quit:
			return
		}
	}
}

// trimCache 清理缓存，实现先shuffle再删除一半的策略
func (s *SQLiteClient) trimCache() {
	// 实现先shuffle再删除一半的策略
	toRemove := len(s.cache) / 2
	if toRemove == 0 && toRemove > s.cacheSize {
		toRemove = 1
	}

	// 将缓存的键收集到切片中
	keys := make([]string, 0, len(s.cache))
	for k := range s.cache {
		keys = append(keys, k)
	}

	// 先对键进行shuffle
	for i := range keys {
		j := rand.Intn(i + 1)
		keys[i], keys[j] = keys[j], keys[i]
	}

	// 删除前一半的键
	for i := 0; i < toRemove && i < len(keys); i++ {
		delete(s.cache, keys[i])
	}
}

// extractKeyParts 从key中提取app名称、cluster和namespace
func (s *SQLiteClient) extractKeyParts(key string) (string, string, string, error) {
	if key == "" {
		return "default", "", "", fmt.Errorf("key cannot be empty")
	}

	parts := strings.Split(key, ":")
	if len(parts) == 0 {
		return "default", "", "", nil // 默认表名，无cluster和namespace
	}
	if len(parts) < 3 {
		return parts[0], "", "", nil // 只有app名称，无cluster和namespace
	}

	// 验证各部分非空
	if parts[0] == "" {
		return "", "", "", fmt.Errorf("invalid key format: app name cannot be empty")
	}
	if parts[1] == "" {
		return "", "", "", fmt.Errorf("invalid key format: cluster cannot be empty")
	}
	if parts[2] == "" {
		return "", "", "", fmt.Errorf("invalid key format: namespace cannot be empty")
	}

	return parts[0], parts[1], parts[2], nil // app名称、cluster、namespace
}
