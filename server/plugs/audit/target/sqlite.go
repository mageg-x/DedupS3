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
package target

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormloger "gorm.io/gorm/logger"

	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/plugs/audit"
)

// SQLiteAuditLog 用于GORM映射的结构体
type SQLiteAuditLog struct {
	ID        int64  `gorm:"primaryKey;autoIncrement" json:"id"`
	EventID   string `gorm:"size:100" json:"event_id"`
	EventTime string `gorm:"size:50;index" json:"event_time"`  // RFC3339格式，添加索引
	EventName string `gorm:"size:100;index" json:"event_name"` // 添加索引
	AccountID string `gorm:"size:100;index" json:"account_id"` // AccountID，添加索引
	RawEntry  string `gorm:"type:TEXT" json:"raw_entry"`
}

// 表名设置
func (SQLiteAuditLog) TableName() string {
	return "audit_logs"
}

// SQLiteTarget 实现AuditTarget接口的SQLite版本
type SQLiteTarget struct {
	db *gorm.DB
}

// NewSQLiteTarget 创建一个SQLite审计目标实例
func NewSQLiteTarget(args *Args) (*SQLiteTarget, error) {
	if args.Driver != "sqlite" {
		return nil, fmt.Errorf("driver must be 'sqlite'")
	}
	if abspath, err := filepath.Abs(args.DSN); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid dsn %s", args.DSN)
		return nil, fmt.Errorf("invalid dsn %s", args.DSN)
	} else {
		args.DSN = abspath
		dir := path.Dir(args.DSN)
		if err = os.MkdirAll(dir, 0755); err != nil {
			logger.GetLogger("dedups3").Errorf("failed permission to create dir %s", args.DSN)
			return nil, fmt.Errorf("failed permission to create dir %s", args.DSN)
		}
	}

	// 打开或创建SQLite数据库
	db, err := gorm.Open(sqlite.Open(args.DSN), &gorm.Config{
		Logger: gormloger.Default.LogMode(gormloger.Silent), // 完全关闭 GORM 内部日志
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// 设置 SQLite 连接参数
	sqlDB.SetMaxOpenConns(10) // SQLite 建议单连接

	// 索引已通过结构体标签定义，AutoMigrate会自动创建
	if err := db.AutoMigrate(&SQLiteAuditLog{}); err != nil {
		return nil, fmt.Errorf("failed to migrate audit_logs table: %w", err)
	}

	return &SQLiteTarget{db: db}, nil
}

// Send 写入单条审计日志到SQLite数据库
func (s *SQLiteTarget) Send(ctx context.Context, entry *audit.Entry) error {
	// 将entry转换为JSON字符串以便存储
	rawEntry, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal audit entry: %w", err)
	}

	// 准备要插入的审计日志记录
	auditLog := &SQLiteAuditLog{
		EventID:   entry.EventID,
		EventTime: entry.EventTime.Format(time.RFC3339),
		EventName: entry.EventName,
		// 使用RecipientAccountID作为AccountID
		AccountID: entry.RecipientAccountID,
		RawEntry:  string(rawEntry),
	}

	// 使用GORM插入记录
	db := s.db.WithContext(ctx)
	if err := db.Create(auditLog).Error; err != nil {
		return fmt.Errorf("failed to insert audit log into SQLite: %w", err)
	}

	return nil
}

// Query 按条件查询审计日志
func (s *SQLiteTarget) Query(ctx context.Context, cond *QueryCondition, opts *QueryOption) (*QueryResult, error) {
	db := s.db.WithContext(ctx)

	// 构建查询
	query := db.Model(&SQLiteAuditLog{})

	// 应用查询条件
	if cond != nil {
		if cond.StartTime != nil {
			query = query.Where("event_time >= ?", cond.StartTime.Format(time.RFC3339))
		}

		if cond.EndTime != nil {
			query = query.Where("event_time <= ?", cond.EndTime.Format(time.RFC3339))
		}

		if cond.EventName != nil && *cond.EventName != "" {
			query = query.Where("event_name = ?", *cond.EventName)
		}

		if cond.UserID != nil && *cond.UserID != "" {
			// UserID可能是用户名或ARN，这里简化处理
			query = query.Where("account_id = ?", *cond.UserID)
		}
	}

	// 查询总记录数
	total := int64(0)
	if err := query.Count(&total).Error; err != nil {
		return nil, fmt.Errorf("failed to count audit logs: %w", err)
	}

	// 设置分页和排序
	offset := 0
	orderBy := "event_time DESC"

	if opts != nil {
		if opts.Offset >= 0 {
			offset = opts.Offset
		}
		if opts.OrderBy != "" {
			orderBy = opts.OrderBy
		}
	}

	maxCount := 1000
	// 查询数据
	var logs []SQLiteAuditLog
	if err := query.Order(orderBy).Limit(maxCount).Offset(offset).Find(&logs).Error; err != nil {
		return nil, fmt.Errorf("failed to query audit logs: %w", err)
	}

	// 转换为Entry对象
	entries := make([]*audit.Entry, 0, len(logs))
	for _, log := range logs {
		// 从RawEntry解析回Entry对象
		var entry audit.Entry
		if err := json.Unmarshal([]byte(log.RawEntry), &entry); err != nil {
			// 如果解析失败，跳过该条记录
			continue
		}
		entries = append(entries, &entry)
	}

	// 判断是否还有更多数据
	hasMore := int64(len(logs)) == total || int64(len(logs)) >= int64(maxCount)

	return &QueryResult{
		Entries: entries,
		HasMore: hasMore,
		Total:   total,
	}, nil
}

// Close 关闭数据库连接
func (s *SQLiteTarget) Close() error {
	if s.db != nil {
		sqlDB, err := s.db.DB()
		if err != nil {
			return err
		}
		return sqlDB.Close()
	}
	return nil
}
