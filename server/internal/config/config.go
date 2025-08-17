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
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"gopkg.in/yaml.v3"
)

var (
	globalConfig atomic.Value
	loadMutex    sync.Mutex
)

// Config 结构体定义所有配置项，字段直接设置默认值
type Config struct {
	Address             string        `yaml:"address" json:"address" env:"BOULDER_ADDRESS"`
	Listeners           int           `yaml:"listeners" json:"listeners" env:"BOULDER_LISTENERS"`
	ConsoleAddress      string        `yaml:"console_address" json:"consoleAddress" env:"BOULDER_CONSOLE_ADDRESS"`
	ShutdownTimeout     time.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout" env:"BOULDER_SHUTDOWN_TIMEOUT"`
	IdleTimeout         time.Duration `yaml:"idle_timeout" json:"idleTimeout" env:"BOULDER_IDLE_TIMEOUT"`
	ReadHeaderTimeout   time.Duration `yaml:"read_header_timeout" json:"readHeaderTimeout" env:"BOULDER_READ_HEADER_TIMEOUT"`
	ConnUserTimeout     time.Duration `yaml:"conn_user_timeout" json:"connUserTimeout" env:"BOULDER_CONN_USER_TIMEOUT"`
	ReadTimeout         time.Duration `yaml:"read_timeout" json:"readTimeout" env:"BOULDER_READ_TIMEOUT"`
	WriteTimeout        time.Duration `yaml:"write_timeout" json:"writeTimeout" env:"BOULDER_WRITE_TIMEOUT"`
	Interface           string        `yaml:"interface" json:"interface" env:"BOULDER_INTERFACE"`
	MaxIdleConnsPerHost int           `yaml:"max_idle_conns_per_host" json:"maxIdleConnsPerHost" env:"BOULDER_MAX_IDLE_CONNS_PER_HOST"`
	Memlimit            string        `yaml:"memlimit" json:"memlimit" env:"BOULDER_MEMLIMIT"`
	SendBufSize         int           `yaml:"send_buf_size" json:"sendBufSize" env:"BOULDER_SEND_BUF_SIZE"`
	RecvBufSize         int           `yaml:"recv_buf_size" json:"recvBufSize" env:"BOULDER_RECV_BUF_SIZE"`
	LogDir              string        `yaml:"log_dir" json:"logDir" env:"BOULDER_LOG_DIR"`
	LogSize             int           `yaml:"log_size" json:"logSize" env:"BOULDER_LOG_SIZE"`
	LogMaxAge           int           `yaml:"log_max_age" json:"logMaxAge" env:"BOULDER_LOG_MAX_AGE"`
	LogMaxBackups       int           `yaml:"log_max_backups" json:"logMaxBackups" env:"BOULDER_LOG_MAX_BACKUPS"`
	LogCompress         bool          `yaml:"log_compress" json:"logCompress" env:"BOULDER_LOG_COMPRESS"`
	Domains             []string      `yaml:"domains" json:"domains" env:"BOULDER_DOMAINS"`
}

// 创建带默认值的配置实例
func DefaultConfig() *Config {
	return &Config{
		Address:             ":3000",
		Listeners:           1,
		ConsoleAddress:      ":3002",
		ShutdownTimeout:     30 * time.Second,
		IdleTimeout:         30 * time.Second,
		ReadHeaderTimeout:   30 * time.Second,
		ConnUserTimeout:     10 * time.Minute,
		ReadTimeout:         10 * time.Second,
		WriteTimeout:        10 * time.Second,
		MaxIdleConnsPerHost: 2048,
		SendBufSize:         4 * 1024 * 1024, // 4MB
		RecvBufSize:         4 * 1024 * 1024,
		LogDir:              "./logs",
		LogSize:             10 * 1024 * 1024, // 10MB
		LogMaxAge:           30,
		LogMaxBackups:       7,
		LogCompress:         true,
		Domains:             []string{"example.com", "example.org"},
	}
}

// Load 加载配置，支持多格式
func load(filepath string) (*Config, error) {
	cfg := DefaultConfig() // 创建带默认值的实例

	// 从配置文件加载（如果提供了文件路径）
	if filepath != "" {
		if err := loadFromFile(cfg, filepath); err != nil {
			return nil, err
		}
	}

	// 从环境变量覆盖
	loadFromEnv(cfg)

	return cfg, nil
}

// 重新加载配置（支持热更新）
func Load(configPath string) error {
	if configPath == "" {
		return nil
	}

	loadMutex.Lock()
	defer loadMutex.Unlock()

	cfg, err := load(configPath)
	if err != nil {
		return err
	}

	globalConfig.Store(cfg)
	return nil
}

// 获取全局配置实例（线程安全）
func Get() Config {
	if cfg := globalConfig.Load(); cfg != nil {
		return *cfg.(*Config)
	}

	// 尝试自动初始化默认配置
	loadMutex.Lock()
	defer loadMutex.Unlock()

	if cfg := globalConfig.Load(); cfg != nil {
		return *cfg.(*Config)
	}

	// 创建默认配置
	defaultCfg := DefaultConfig()
	globalConfig.Store(defaultCfg)
	return *defaultCfg
}

// 根据文件扩展名自动选择解析器
func loadFromFile(cfg *Config, path string) error {
	file, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		return yaml.Unmarshal(file, cfg)
	case ".json":
		return json.Unmarshal(file, cfg)
	case ".properties", ".props", ".conf":
		return loadProperties(file, cfg)
	default:
		// 尝试自动检测格式
		if err := yaml.Unmarshal(file, cfg); err == nil {
			return nil
		}
		if err := json.Unmarshal(file, cfg); err == nil {
			return nil
		}
		return loadProperties(file, cfg)
	}
}

// 解析 properties 格式
func loadProperties(data []byte, cfg *Config) error {
	props, err := properties.Load(data, properties.UTF8)
	if err != nil {
		return err
	}

	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := v.Field(i)

		// 获取可能的标签名
		tags := []string{
			field.Tag.Get("yaml"),
			field.Tag.Get("json"),
			strings.ToLower(field.Name),
		}

		// 查找存在的属性键
		var propKey string
		for _, tag := range tags {
			if tag != "" && props.GetString(tag, "") != "" {
				propKey = tag
				break
			}
		}
		if propKey == "" {
			continue
		}

		propValue := props.GetString(propKey, "")
		if propValue == "" {
			continue
		}

		setFieldValue(fieldVal, propValue)
	}
	return nil
}

// 从环境变量加载
func loadFromEnv(cfg *Config) {
	t := reflect.TypeOf(*cfg)
	v := reflect.ValueOf(cfg).Elem()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		envTag := field.Tag.Get("env")
		if envTag == "" {
			continue
		}

		envValue := os.Getenv(envTag)
		if envValue == "" {
			continue
		}

		fieldVal := v.Field(i)
		setFieldValue(fieldVal, envValue)
	}
}

// 通用字段值设置方法
func setFieldValue(field reflect.Value, value string) {
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Int, reflect.Int64:
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			field.SetInt(intVal)
		}
	case reflect.Bool:
		if boolVal, err := strconv.ParseBool(value); err == nil {
			field.SetBool(boolVal)
		}
	case reflect.Float64:
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			field.SetFloat(floatVal)
		}
	case reflect.Struct:
		// 特别处理 time.Duration 类型
		if field.Type().String() == "time.Duration" {
			if d, err := time.ParseDuration(value); err == nil {
				field.Set(reflect.ValueOf(d))
			}
		}
	}
}
