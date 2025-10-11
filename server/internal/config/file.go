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
// 存放本机配置

package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creasty/defaults"
	"github.com/spf13/viper"

	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/utils"
)

var (
	globalConfig atomic.Value
	loadMutex    sync.Mutex
)

// 全局变量
var (
	GlobalNodeID = ""
	IsDev        = isDevelopment()
)

type IAMConfig struct {
	Username string `json:"username" mapstructure:"username" env:"IAM_USERNAME" default:"boulder"`
	Password string `json:"password" mapstructure:"password" env:"IAM_PASSWORD" default:"Abcd@1234"`
	AK       string `json:"access_key" mapstructure:"access_key" env:"IAM_ACCESS_KEY" default:"GGP5NTUY9WRH5NS78UVU"`
	SK       string `json:"secret_key" mapstructure:"secret_key" env:"IAM_SECRET_KEY" default:"5oj6y3Jy7MO4Y2FTI5dOUvCbnOZf8mQGvbCqGN4I"`
}

// TiKVConfig TiKV集群配置
type TiKVConfig struct {
	PDAddrs []string `mapstructure:"pd_addrs" json:"pdAddrs" env:"DEDUPS3_KV_TIKV_PD_ADDRS"`
}

type BadgerConfig struct {
	Path string `mapstructure:"path" json:"path" env:"DEDUPS3_KV_BADGER_PATH" default:"./data/kv"`
}

// KVConfig 存储KV相关配置
type KVConfig struct {
	TiKV *TiKVConfig `mapstructure:"tikv" json:"tikv"`
}

// S3Config S3存储配置
type S3Config struct {
	AccessKey    string `mapstructure:"access_key" json:"accessKey" env:"DEDUPS3_BLOCK_S3_ACCESS_KEY"`
	SecretKey    string `mapstructure:"secret_key" json:"secretKey" env:"DEDUPS3_BLOCK_S3_SECRET_KEY"`
	Region       string `mapstructure:"region" json:"region" env:"DEDUPS3_BLOCK_S3_REGION" default:"us-east-1"`
	Endpoint     string `mapstructure:"endpoint" json:"endpoint" env:"DEDUPS3_BLOCK_S3_ENDPOINT"`
	Bucket       string `mapstructure:"bucket" json:"bucket" env:"DEDUPS3_BLOCK_S3_BUCKET" default:"blocks"`
	UsePathStyle bool   `mapstructure:"use_path_style" json:"usePathStyle" env:"DEDUPS3_BLOCK_S3_USE_PATH_STYLE" default:"true"`
}

func (s *S3Config) Validate() error {
	if s.AccessKey == "" {
		return fmt.Errorf("config error: missing s3.access_key")
	}
	if s.SecretKey == "" {
		return fmt.Errorf("config error: missing s3.secret_key")
	}
	if s.Bucket == "" {
		return fmt.Errorf("config error: missing s3.bucket")
	}
	if s.Region == "" {
		return fmt.Errorf("config error: missing s3.region")
	}

	// 仅做基本合法性检查（可选）
	if s.Endpoint != "" {
		if strings.TrimSpace(s.Endpoint) == "" {
			return fmt.Errorf("config error: s3.endpoint cannot be blank")
		}
		if strings.Contains(s.Endpoint, " ") {
			return fmt.Errorf("config error: s3.endpoint cannot contain spaces")
		}
	}

	return nil
}

func (s *S3Config) Equal(other *S3Config) bool {
	if s == nil && other == nil {
		return true
	}
	if s == nil || other == nil {
		return false
	}

	return s.AccessKey == other.AccessKey &&
		s.SecretKey == other.SecretKey &&
		s.Region == other.Region &&
		s.Endpoint == other.Endpoint &&
		s.Bucket == other.Bucket &&
		s.UsePathStyle == other.UsePathStyle
}

type DiskConfig struct {
	Path string `mapstructure:"path" json:"path" env:"DEDUPS3_BLOCK_DISK_PATH" default:"./data/block"`
}

func (d *DiskConfig) Validate() error {
	if d.Path == "" {
		return fmt.Errorf("config error: cannot empty disk path")
	}
	if absPath, err := filepath.Abs(d.Path); err != nil {
		return fmt.Errorf("config error:  invalid disk path: %s", d.Path)
	} else {
		d.Path = absPath
	}
	return nil
}

func (d *DiskConfig) Equal(other *DiskConfig) bool {
	if d == nil && other == nil {
		return true
	}
	if d == nil || other == nil {
		return false
	}

	// 转为绝对路径并 clean 后比较
	path1, _ := filepath.Abs(d.Path)
	path2, _ := filepath.Abs(other.Path)
	return path1 == path2
}

// StorageConfig 存储Block相关配置
type StorageConfig struct {
	ID        string `mapstructure:"id" json:"id" env:"DEDUPS3_BLOCK_STORAGE_ID"`
	Class     string `mapstructure:"class" json:"class" env:"DEDUPS3_STORAGE_CLASS" default:"STANDARD"`
	Compress  bool   `mapstructure:"compress" json:"compress" env:"DEDUPS3_BLOCK_COMPRESS" default:"true"`
	Encrypt   bool   `mapstructure:"encryte" json:"encrypt" env:"DEDUPS3_BLOCK_ENCRYPT" default:"true"`
	ChunkSize int    `mapstructure:"chunk_size" json:"chunkSize" env:"DEDUPS3_BLOCK_CHUNK_SIZE" default:"2097152"`
	FixChunk  bool   `mapstructure:"fix_chunk"json:"fixChunk" env:"DEDUPS3_BLOCK_FIX_CHUNK" default:"false"`

	// Only one of the following should be set
	S3   *S3Config   `mapstructure:"s3" json:"s3,omitempty"`
	Disk *DiskConfig `mapstructure:"disk" json:"disk,omitempty"`
}

func (s *StorageConfig) Validate() error {
	// S3 和 disk 只能择其一
	if s.S3 != nil && s.Disk != nil {
		return fmt.Errorf("config error: cannot specify both 's3' and 'disk' storage of class %s", s.Class)
	}

	if s.S3 == nil && s.Disk == nil {
		return fmt.Errorf("config error: must have one configured 's3' or 'disk' storage of class %s", s.Class)
	}

	if s.Disk != nil {
		if err := s.Disk.Validate(); err != nil {
			return err
		}
	}

	if s.S3 != nil {
		if err := s.S3.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (s *StorageConfig) Equal(other *StorageConfig) bool {
	if other == nil {
		return false
	}
	if s.Class != other.Class {
		return false
	}

	if s.S3 != nil && other.S3 != nil && !s.S3.Equal(other.S3) {
		return false
	}
	if s.Disk != nil && other.Disk != nil && !s.Disk.Equal(other.Disk) {
		return false
	}

	if (s.S3 != nil && other.S3 == nil) || (s.S3 == nil && other.S3 != nil) {
		return false
	}
	if (s.Disk != nil && other.Disk == nil) || (s.Disk == nil && other.Disk != nil) {
		return false
	}
	return s.S3.Equal(other.S3)
}

// RedisConfig Redis集群配置
type RedisConfig struct {
	Addrs    []string `mapstructure:"addrs" json:"addrs" env:"DEDUPS3_CACHE_REDIS_ADDRS"`
	Password string   `mapstructure:"password" json:"password" env:"DEDUPS3_CACHE_REDIS_PASSWORD"`
	DB       int      `mapstructure:"db" json:"db" env:"DEDUPS3_CACHE_REDIS_DB"`
	PoolSize int      `mapstructure:"pool_size" json:"poolSize" env:"DEDUPS3_CACHE_REDIS_POOL_SIZE"`
}

// CacheConfig 存储Cache相关配置
type CacheConfig struct {
	Redis *RedisConfig `mapstructure:"redis" json:"redis"`
}

// ServerConfig represents server configuration
type ServerConfig struct {
	Address             string        `mapstructure:"address" json:"address" env:"DEDUPS3_SERVER_ADDRESS" default:":3000"`
	Listeners           int           `mapstructure:"listeners" json:"listeners" env:"DEDUPS3_SERVER_LISTENERS" default:"4"`
	ConsoleAddress      string        `mapstructure:"console_address" json:"consoleAddress" env:"DEDUPS3_SERVER_CONSOLE_ADDRESS" default:":3002"`
	ShutdownTimeout     time.Duration `mapstructure:"shutdown_timeout" json:"shutdownTimeout" env:"DEDUPS3_SERVER_SHUTDOWN_TIMEOUT" default:"30s"`
	IdleTimeout         time.Duration `mapstructure:"idle_timeout" json:"idleTimeout" env:"DEDUPS3_SERVER_IDLE_TIMEOUT" default:"30s"`
	ReadHeaderTimeout   time.Duration `mapstructure:"read_header_timeout" json:"readHeaderTimeout" env:"DEDUPS3_SERVER_READ_HEADER_TIMEOUT" default:"30s"`
	ConnUserTimeout     time.Duration `mapstructure:"conn_user_timeout" json:"connUserTimeout" env:"DEDUPS3_SERVER_CONN_USER_TIMEOUT" default:"10m"`
	ReadTimeout         time.Duration `mapstructure:"read_timeout" json:"readTimeout" env:"DEDUPS3_SERVER_READ_TIMEOUT" default:"10s"`
	WriteTimeout        time.Duration `mapstructure:"write_timeout" json:"writeTimeout" env:"DEDUPS3_SERVER_WRITE_TIMEOUT" default:"10s"`
	Interface           string        `mapstructure:"interface" json:"interface" env:"DEDUPS3_SERVER_INTERFACE" default:""`
	MaxIdleConnsPerHost int           `mapstructure:"max_idle_conns_per_host" json:"maxIdleConnsPerHost" env:"DEDUPS3_SERVER_MAX_IDLE_CONNS_PER_HOST" default:"2048"`
	Memlimit            string        `mapstructure:"memlimit" json:"memlimit" env:"DEDUPS3_SERVER_MEMLIMIT" default:"8589934592"`
	SendBufSize         int           `mapstructure:"send_buf_size" json:"sendBufSize" env:"DEDUPS3_SERVER_SEND_BUF_SIZE" default:"8388608"`
	RecvBufSize         int           `mapstructure:"recv_buf_size" json:"recvBufSize" env:"DEDUPS3_SERVER_RECV_BUF_SIZE" default:"8388608"`
	Domains             []string      `mapstructure:"domains" json:"domains" env:"DEDUPS3_DOMAINS" `
}

// LogConfig represents log configuration
type LogConfig struct {
	Dir        string `mapstructure:"dir" json:"dir" env:"DEDUPS3_LOG_DIR" default:"./logs"`
	Size       int    `mapstructure:"size" json:"size" env:"DEDUPS3_LOG_SIZE" default:"10485760"`
	MaxAge     int    `mapstructure:"max_age" json:"maxAge" env:"DEDUPS3_LOG_MAX_AGE" default:"30"`
	MaxBackups int    `mapstructure:"max_backups" json:"maxBackups" env:"DEDUPS3_LOG_MAX_BACKUPS" default:"7"`
	Compress   bool   `mapstructure:"compress" json:"compress" env:"DEDUPS3_LOG_COMPRESS" default:"true"`
}

type BlockConfig struct {
	MaxRetentionTime time.Duration `mapstructure:"max_retention_time" json:"maxRetentionTime"  env:"DEDUPS3_BLOCK_MAX_RETENTION_TIME" default:"1h"`
	SyncNum          int           `mapstructure:"sync_num" json:"syncNum" env:"DEDUPS3_BLOCK_SYNC_NUM" default:"1"`
	SyncDelay        time.Duration `mapstructure:"sync_delay" json:"syncDelay" evn:"DEDUPS3_BLOCK_SYNC_DELAY" default:"10s"`
	ShardNum         int           `mapstructure:"shard_num" json:"shardNum" env:"DEDUPS3_BLOCK_SHARD_NUM" default:"10"`
	MaxSize          int           `mapstructure:"max_size" json:"maxSize" env:"DEDUPS3_BLOCK_MAX_SIZE" default:"67108864"`
	MaxHeadSize      int           `mapstructure:"max_head_size" json:"maxHeadSize" env:"DEDUPS3_BLOCK_MAX_HEAD_SIZE" default:"204800"`
}

type NodeConfig struct {
	LocalNode string `mapstructure:"local_node" json:"localNode" env:"DEDUPS3_LOCAL_NODE" default:"http://127.0.0.1:3000"`
	LocalDir  string `mapstructure:"local_dir" json:"localDir" env:"DEDUPS3_LOCAL_DIR" default:"./data"`
	Region    string `mapstructure:"region" json:"region" env:"DEDUPS3_REGION" default:"us-east-1"`
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
	cfg.Node.LocalDir, _ = filepath.Abs(cfg.Node.LocalDir)
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

// IsDev 表示是否是开发环境
func isDevelopment() bool {
	env := strings.ToLower(os.Getenv("ENV"))
	return env == "dev" || env == "development"
}
