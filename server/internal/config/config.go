// Package config /*
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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/mageg-x/boulder/internal/utils"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mageg-x/boulder/internal/logger"

	"github.com/creasty/defaults"
	"github.com/spf13/viper"
)

var (
	globalConfig atomic.Value
	loadMutex    sync.Mutex
)

type IAMConfig struct {
	Username string `json:"username" mapstructure:"username" env:"IAM_USERNAME" default:"stevenrao"`
	Password string `json:"password" mapstructure:"password" env:"IAM_PASSWORD" default:"Abcd@1234"`
	AK       string `json:"access_key" mapstructure:"access_key" env:"IAM_ACCESS_KEY" default:"GGP5NTUY9WRH5NS78UVU"`
	SK       string `json:"secret_key" mapstructure:"secret_key" env:"IAM_SECRET_KEY" default:"5oj6y3Jy7MO4Y2FTI5dOUvCbnOZf8mQGvbCqGN4I"`
}

// TiKVConfig TiKV集群配置
type TiKVConfig struct {
	PDAddrs []string `mapstructure:"pd_addrs" json:"pdAddrs" env:"BOULDER_KV_TIKV_PD_ADDRS"`
}

type BadgerConfig struct {
	Path string `mapstructure:"path" json:"path" env:"BOULDER_KV_BADGER_PATH" default:"./data/kv"`
}

// KVConfig 存储KV相关配置
type KVConfig struct {
	TiKV *TiKVConfig `mapstructure:"tikv" json:"tikv"`
}

// S3Config S3存储配置
type S3Config struct {
	AccessKey    string `mapstructure:"access_key" json:"accessKey" env:"BOULDER_BLOCK_S3_ACCESS_KEY"`
	SecretKey    string `mapstructure:"secret_key" json:"secretKey" env:"BOULDER_BLOCK_S3_SECRET_KEY"`
	Region       string `mapstructure:"region" json:"region" env:"BOULDER_BLOCK_S3_REGION" default:"us-east-1"`
	Endpoint     string `mapstructure:"endpoint" json:"endpoint" env:"BOULDER_BLOCK_S3_ENDPOINT"`
	Bucket       string `mapstructure:"bucket" json:"bucket" env:"BOULDER_BLOCK_S3_BUCKET" default:"blocks"`
	UsePathStyle bool   `mapstructure:"use_path_style" json:"usePathStyle" env:"BOULDER_BLOCK_S3_USE_PATH_STYLE" default:"true"`
}

type DiskConfig struct {
	Path string `mapstructure:"path" json:"path" env:"BOULDER_BLOCK_DISK_PATH" default:"./data/block"`
}

// StorageConfig 存储Block相关配置
type StorageConfig struct {
	S3   *S3Config   `mapstructure:"s3" json:"s3"`
	Disk *DiskConfig `mapstructure:"disk" json:"disk"`
}

// RedisConfig Redis集群配置
type RedisConfig struct {
	Addrs    []string `mapstructure:"addrs" json:"addrs" env:"BOULDER_CACHE_REDIS_ADDRS"`
	Password string   `mapstructure:"password" json:"password" env:"BOULDER_CACHE_REDIS_PASSWORD"`
	DB       int      `mapstructure:"db" json:"db" env:"BOULDER_CACHE_REDIS_DB"`
	PoolSize int      `mapstructure:"pool_size" json:"poolSize" env:"BOULDER_CACHE_REDIS_POOL_SIZE"`
}

// CacheConfig 存储Cache相关配置
type CacheConfig struct {
	Redis *RedisConfig `mapstructure:"redis" json:"redis"`
}

// ServerConfig represents server configuration
type ServerConfig struct {
	Address             string        `mapstructure:"address" json:"address" env:"BOULDER_SERVER_ADDRESS" default:":3000"`
	Listeners           int           `mapstructure:"listeners" json:"listeners" env:"BOULDER_SERVER_LISTENERS" default:"4"`
	ConsoleAddress      string        `mapstructure:"console_address" json:"consoleAddress" env:"BOULDER_SERVER_CONSOLE_ADDRESS" default:":3002"`
	ShutdownTimeout     time.Duration `mapstructure:"shutdown_timeout" json:"shutdownTimeout" env:"BOULDER_SERVER_SHUTDOWN_TIMEOUT" default:"30s"`
	IdleTimeout         time.Duration `mapstructure:"idle_timeout" json:"idleTimeout" env:"BOULDER_SERVER_IDLE_TIMEOUT" default:"30s"`
	ReadHeaderTimeout   time.Duration `mapstructure:"read_header_timeout" json:"readHeaderTimeout" env:"BOULDER_SERVER_READ_HEADER_TIMEOUT" default:"30s"`
	ConnUserTimeout     time.Duration `mapstructure:"conn_user_timeout" json:"connUserTimeout" env:"BOULDER_SERVER_CONN_USER_TIMEOUT" default:"10m"`
	ReadTimeout         time.Duration `mapstructure:"read_timeout" json:"readTimeout" env:"BOULDER_SERVER_READ_TIMEOUT" default:"10s"`
	WriteTimeout        time.Duration `mapstructure:"write_timeout" json:"writeTimeout" env:"BOULDER_SERVER_WRITE_TIMEOUT" default:"10s"`
	Interface           string        `mapstructure:"interface" json:"interface" env:"BOULDER_SERVER_INTERFACE" default:""`
	MaxIdleConnsPerHost int           `mapstructure:"max_idle_conns_per_host" json:"maxIdleConnsPerHost" env:"BOULDER_SERVER_MAX_IDLE_CONNS_PER_HOST" default:"2048"`
	Memlimit            string        `mapstructure:"memlimit" json:"memlimit" env:"BOULDER_SERVER_MEMLIMIT" default:"8589934592"`
	SendBufSize         int           `mapstructure:"send_buf_size" json:"sendBufSize" env:"BOULDER_SERVER_SEND_BUF_SIZE" default:"8388608"`
	RecvBufSize         int           `mapstructure:"recv_buf_size" json:"recvBufSize" env:"BOULDER_SERVER_RECV_BUF_SIZE" default:"8388608"`
	Domains             []string      `mapstructure:"domains" json:"domains" env:"BOULDER_DOMAINS" `
}

// LogConfig represents log configuration
type LogConfig struct {
	Dir        string `mapstructure:"dir" json:"dir" env:"BOULDER_LOG_DIR" default:"./logs"`
	Size       int    `mapstructure:"size" json:"size" env:"BOULDER_LOG_SIZE" default:"10485760"`
	MaxAge     int    `mapstructure:"max_age" json:"maxAge" env:"BOULDER_LOG_MAX_AGE" default:"30"`
	MaxBackups int    `mapstructure:"max_backups" json:"maxBackups" env:"BOULDER_LOG_MAX_BACKUPS" default:"7"`
	Compress   bool   `mapstructure:"compress" json:"compress" env:"BOULDER_LOG_COMPRESS" default:"true"`
}

type BlockConfig struct {
	MaxRetentionTime time.Duration `mapstructure:"max_retention_time" json:"maxRetentionTime"  env:"BOULDER_BLOCK_MAX_RETENTION_TIME" default:"1h"`
	SyncNum          int           `mapstructure:"sync_num" json:"syncNum" env:"BOULDER_BLOCK_SYNC_NUM" default:"1"`
	SyncDelay        time.Duration `mapstructure:"sync_delay" json:"syncDelay" evn:"BOULDER_BLOCK_SYNC_DELAY" default:"10s"`
	ShardNum         int           `mapstructure:"shard_num" json:"shardNum" env:"BOULDER_BLOCK_SHARD_NUM" default:"10"`
	MaxSize          int           `mapstructure:"max_size" json:"maxSize" env:"BOULDER_BLOCK_MAX_SIZE" default:"67108864"`
	MaxHeadSize      int           `mapstructure:"max_head_size" json:"maxHeadSize" env:"BOULDER_BLOCK_MAX_HEAD_SIZE" default:"204800"`
	Compress         bool          `mapstructure:"compress" json:"compress" env:"BOULDER_BLOCK_COMPRESS" default:"true"`
	Encrypt          bool          `mapstructure:"encryte" json:"encrypt" env:"BOULDER_BLOCK_ENCRYPT" default:"true"`
	ChunkSize        int           `mapstructure:"chunk_size" json:"chunkSize" env:"BOULDER_BLOCK_CHUNK_SIZE" default:"67108864"`
}

type NodeConfig struct {
	LocalNode string `mapstructure:"local_node" json:"localNode" env:"BOULDER_LOCAL_NODE" default:"http://127.0.0.1:3000"`
	LocalDir  string `mapstructure:"local_dir" json:"localDir" env:"BOULDER_LOCAL_DIR" default:"./data"`
}

type Config struct {
	Server ServerConfig `mapstructure:"server" json:"server"`
	Log    LogConfig    `mapstructure:"log" json:"log"`
	KV     KVConfig     `mapstructure:"kv" json:"kv"`
	Cache  CacheConfig  `mapstructure:"cache" json:"cache"`
	Iam    IAMConfig    `mapstructure:"iam" json:"iam"`
	Block  BlockConfig  `mapstructure:"block" json:"block"`
	Node   NodeConfig   `mapstructure:"node" json:"node"`
}

// DefaultConfig 创建带默认值的配置实例
func DefaultConfig() *Config {
	cfg := &Config{}
	if err := defaults.Set(cfg); err != nil {
		panic("BUG: failed to set defaults on Config: " + err.Error())
	}

	// creasty/defaults 对数组支持不好，手动设置 slice,
	cfg.Server.Domains = []string{"example.com", "example.org"}
	return cfg
}

// Load 加载配置，支持多格式
func load(path string) *Config {
	_cfg := DefaultConfig()

	if !utils.FileExists(path) {
		logger.GetLogger("").Warnf("config file not found: %s, using default config", path)
		return _cfg
	}

	v := viper.New()
	v.SetConfigFile(path)
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		v.SetConfigType("yaml")
	case ".json":
		v.SetConfigType("json")
	case ".properties", ".props":
		v.SetConfigType("properties")
	default:
		logger.GetLogger("").Errorf("unknown config file type: %s", ext)
		return _cfg
	}
	v.AutomaticEnv()
	if err := v.ReadInConfig(); err != nil {
		logger.GetLogger("").Errorf("failed to read config file: %v", err)
		return _cfg
	}

	var cfg Config
	// 设置 default 标签的值
	if err := defaults.Set(&cfg); err != nil {
		logger.GetLogger("").Errorf("failed to set defaults for config file %s: %s", path, err.Error())
		return _cfg
	}
	// 解码到结构体
	if err := v.Unmarshal(&cfg); err != nil {
		logger.GetLogger("").Errorf("failed to unmarshal config file %s: %s", path, err.Error())
		return _cfg
	}

	return &cfg
}

// Load 重新加载配置（支持热更新）
func Load(configPath string) error {
	if configPath == "" {
		defaultCfg := DefaultConfig()
		globalConfig.Store(defaultCfg)
		return nil // 明确表示“成功加载了默认配置”
	}

	if !utils.FileExists(configPath) {
		defaultCfg := DefaultConfig()
		globalConfig.Store(defaultCfg)
		logger.GetLogger("").Warnf("config file %s not found, using default config", configPath)
		return nil // 成功使用默认值
	}

	// ... 正常加载
	cfg := load(configPath)
	if cfg == nil {
		return fmt.Errorf("failed to load config from %s", configPath)
	}

	// 对一些 调用频繁的数据进行 规范
	cfg.Node.LocalDir = filepath.Clean(cfg.Node.LocalDir)
	cfg.Node.LocalNode = strings.TrimSpace(cfg.Node.LocalNode)
	globalConfig.Store(cfg)

	// 初始化一些全局变量
	nodeId := sha256.Sum256([]byte(cfg.Node.LocalNode))
	GlobalNodeID = hex.EncodeToString(nodeId[:])

	return nil
}

// Get 获取全局配置实例（线程安全）
func Get() Config {
	if c, ok := globalConfig.Load().(*Config); ok && c != nil {
		return *c
	}

	loadMutex.Lock()
	defer loadMutex.Unlock()

	// 只需再查一次，避免重复初始化
	if c, ok := globalConfig.Load().(*Config); ok && c != nil {
		return *c
	}

	// 第一次访问，初始化默认配置
	cfg := DefaultConfig()
	globalConfig.Store(cfg)
	return *cfg
}
