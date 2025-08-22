// Package config /*
package config

import (
	"fmt"
	"github.com/mageg-x/boulder/internal/logger"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creasty/defaults"
	"github.com/spf13/viper"
)

var (
	globalConfig atomic.Value
	loadMutex    sync.Mutex
)

// TiKVConfig TiKV集群配置
type TiKVConfig struct {
	PDAddrs []string `yaml:"pd_addrs" json:"pdAddrs" env:"BOULDER_KV_TIKV_PD_ADDRS"`
}
type BadgerConfig struct {
	Path string `yaml:"path" json:"path" env:"BOULDER_KV_BADGER_PATH" default:"./data/kv"`
}

// KVConfig 存储KV相关配置
type KVConfig struct {
	TiKV   *TiKVConfig  `yaml:"tikv" json:"tikv"`
	Badger BadgerConfig `yaml:"badger" json:"badger"`
}

// S3Config S3存储配置
type S3Config struct {
	AccessKey    string `yaml:"access_key" json:"accessKey" env:"BOULDER_BLOCK_S3_ACCESS_KEY"`
	SecretKey    string `yaml:"secret_key" json:"secretKey" env:"BOULDER_BLOCK_S3_SECRET_KEY"`
	Region       string `yaml:"region" json:"region" env:"BOULDER_BLOCK_S3_REGION" default:"us-east-1"`
	Endpoint     string `yaml:"endpoint" json:"endpoint" env:"BOULDER_BLOCK_S3_ENDPOINT"`
	Bucket       string `yaml:"bucket" json:"bucket" env:"BOULDER_BLOCK_S3_BUCKET" default:"blocks"`
	UsePathStyle bool   `yaml:"usePathStyle" json:"usePathStyle" env:"BOULDER_BLOCK_S3_USE_PATH_STYLE" default:"true"`
}

type DiskConfig struct {
	Path string `yaml:"path" json:"path" env:"BOULDER_BLOCK_DISK_PATH" default:"./data/block"`
}

// BlockConfig 存储Block相关配置
type BlockConfig struct {
	S3   *S3Config  `yaml:"s3" json:"s3"`
	Disk DiskConfig `yaml:"disk" json:"disk"`
}

// RedisConfig Redis集群配置
type RedisConfig struct {
	Addrs    []string `yaml:"addrs" json:"addrs" env:"BOULDER_CACHE_REDIS_ADDRS"`
	Password string   `yaml:"password" json:"password" env:"BOULDER_CACHE_REDIS_PASSWORD"`
	DB       int      `yaml:"db" json:"db" env:"BOULDER_CACHE_REDIS_DB"`
	PoolSize int      `yaml:"pool_size" json:"poolSize" env:"BOULDER_CACHE_REDIS_POOL_SIZE"`
}

// CacheConfig 存储Cache相关配置
type CacheConfig struct {
	Redis *RedisConfig `yaml:"redis" json:"redis"`
}

// ServerConfig represents server configuration
type ServerConfig struct {
	Address             string        `yaml:"address" json:"address" env:"BOULDER_SERVER_ADDRESS" default:":3000"`
	Listeners           int           `yaml:"listeners" json:"listeners" env:"BOULDER_SERVER_LISTENERS" default:"1"`
	ConsoleAddress      string        `yaml:"console_address" json:"consoleAddress" env:"BOULDER_SERVER_CONSOLE_ADDRESS" default:":3002"`
	ShutdownTimeout     time.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout" env:"BOULDER_SERVER_SHUTDOWN_TIMEOUT" default:"30s"`
	IdleTimeout         time.Duration `yaml:"idle_timeout" json:"idleTimeout" env:"BOULDER_SERVER_IDLE_TIMEOUT" default:"30s"`
	ReadHeaderTimeout   time.Duration `yaml:"read_header_timeout" json:"readHeaderTimeout" env:"BOULDER_SERVER_READ_HEADER_TIMEOUT" default:"30s"`
	ConnUserTimeout     time.Duration `yaml:"conn_user_timeout" json:"connUserTimeout" env:"BOULDER_SERVER_CONN_USER_TIMEOUT" default:"10m"`
	ReadTimeout         time.Duration `yaml:"read_timeout" json:"readTimeout" env:"BOULDER_SERVER_READ_TIMEOUT" default:"10s"`
	WriteTimeout        time.Duration `yaml:"write_timeout" json:"writeTimeout" env:"BOULDER_SERVER_WRITE_TIMEOUT" default:"10s"`
	Interface           string        `yaml:"interface" json:"interface" env:"BOULDER_SERVER_INTERFACE" default:""`
	MaxIdleConnsPerHost int           `yaml:"max_idle_conns_per_host" json:"maxIdleConnsPerHost" env:"BOULDER_SERVER_MAX_IDLE_CONNS_PER_HOST" default:"2048"`
	Memlimit            string        `yaml:"memlimit" json:"memlimit" env:"BOULDER_SERVER_MEMLIMIT" default:"8589934592"`
	SendBufSize         int           `yaml:"send_buf_size" json:"sendBufSize" env:"BOULDER_SERVER_SEND_BUF_SIZE" default:"4194304"`
	RecvBufSize         int           `yaml:"recv_buf_size" json:"recvBufSize" env:"BOULDER_SERVER_RECV_BUF_SIZE" default:"4194304"`
	Domains             []string      `yaml:"domains" json:"domains" env:"BOULDER_DOMAINS" `
}

// LogConfig represents log configuration
type LogConfig struct {
	Dir        string `yaml:"dir" json:"dir" env:"BOULDER_LOG_DIR" default:"./logs"`
	Size       int    `yaml:"size" json:"size" env:"BOULDER_LOG_SIZE" default:"10485760"`
	MaxAge     int    `yaml:"max_age" json:"maxAge" env:"BOULDER_LOG_MAX_AGE" default:"30"`
	MaxBackups int    `yaml:"max_backups" json:"maxBackups" env:"BOULDER_LOG_MAX_BACKUPS" default:"7"`
	Compress   bool   `yaml:"compress" json:"compress" env:"BOULDER_LOG_COMPRESS" default:"true"`
}

type Config struct {
	Server ServerConfig `yaml:"server" json:"server"`
	Log    LogConfig    `yaml:"log" json:"log"`
	KV     KVConfig     `yaml:"kv" json:"kv"`
	Block  BlockConfig  `yaml:"block" json:"block"`
	Cache  CacheConfig  `yaml:"cache" json:"cache"`
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

	if _, err := os.Stat(path); os.IsNotExist(err) {
		logger.GetLogger("").Warnf("config file not found: %s, using default config", path)
		return _cfg
	} else if err != nil {
		logger.GetLogger("").Errorf("failed to stat config file %s: %v", path, err)
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
	// 解码到结构体
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		logger.GetLogger("").Errorf("failed to unmarshal config file %s: %s", path, err.Error())
		return _cfg
	}

	// 设置 default 标签的值
	if err := defaults.Set(&cfg); err != nil {
		logger.GetLogger("").Errorf("failed to set defaults for config file %s: %s", path, err.Error())
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

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
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
	globalConfig.Store(cfg)
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
