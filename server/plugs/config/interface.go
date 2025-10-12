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
// 存放云配置, 譬如 IAM， Storage 这些配置

package config

import (
	"fmt"
	"github.com/mageg-x/dedups3/internal/logger"
)

type Args struct {
	Driver    string `json:"driver"` // "sqlite" 或 "http"
	DSN       string `json:"dsn"`    // SQLite文件路径 或 HTTP服务URL
	AuthToken string `json:"auth_token,omitempty"`
}

type KVConfigClient interface {
	Open(args *Args) error
	Close() error

	// 普通数据操作接口
	Get(key string, tpl interface{}) (interface{}, error)
	Set(key string, value interface{}) error
	Del(key string) error
	Create(key string, value interface{}) error
	List(prefix, marker string, limit int, tpl interface{}) (map[string]interface{}, string, error)

	// 支持事务数据操作接口
	TxnBegin() (string, error)
	TxnCommit(sessionID string) error
	TxnRollback(sessionID string) error
	TxnGetKv(sessionID string, key string, tpl interface{}) (interface{}, error)
	TxnSetKv(sessionID string, key string, value interface{}) error
	TxnDelKv(sessionID string, key string) error
	TxnCreateKv(sessionID string, key string, value interface{}) error
	TxnListKv(sessionID string, prefix, marker string, limit int, tpl interface{}) (map[string]interface{}, string, error)
}

// 创建 kvconfig 实例
func NewKVConfig(args *Args) (KVConfigClient, error) {
	if args == nil {
		return nil, fmt.Errorf("args is nil")
	}
	var impl KVConfigClient
	switch args.Driver {
	case "sqlite":
		impl = NewSQLiteClient()
	case "http":
		impl = nil
	default:
		return nil, fmt.Errorf("unsupported driver: %s", args.Driver)
	}

	if impl == nil {
		return nil, fmt.Errorf("failed new driver: %s", args.Driver)
	}

	if err := impl.Open(args); err != nil {
		logger.GetLogger("dedups3").Errorf("failed opening config %#v: %v", args, err)
		return nil, err
	}

	return impl, nil
}
