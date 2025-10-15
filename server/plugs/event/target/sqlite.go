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
	"github.com/mageg-x/dedups3/plugs/event"
)

// SQLiteEventLog 用于GORM映射的结构体
type SQLiteEventLog struct {
	ID        int64  `gorm:"primaryKey;autoIncrement" json:"id"`
	EventID   string `gorm:"size:100" json:"event_id"`
	EventTime string `gorm:"size:50;index" json:"event_time"`  // RFC3339格式，添加索引
	EventName string `gorm:"size:100;index" json:"event_name"` // 添加索引
	UserID    string `gorm:"size:100;index" json:"user_id"`    // UserID，添加索引
	RawRecord string `gorm:"type:TEXT" json:"raw_record"`
}

// 表名设置
func (SQLiteEventLog) TableName() string {
	return "event_logs"
}

// SQLiteTarget 实现EventTarget接口的SQLite版本
type SQLiteTarget struct {
	db *gorm.DB
}

// NewSQLiteTarget 创建一个SQLite事件目标实例
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
	if err := db.AutoMigrate(&SQLiteEventLog{}); err != nil {
		return nil, fmt.Errorf("failed to migrate event_logs table: %w", err)
	}

	return &SQLiteTarget{db: db}, nil
}

// Send 写入单条事件日志到SQLite数据库
func (s *SQLiteTarget) Send(ctx context.Context, record *event.Record) error {
	// 将record转换为JSON字符串以便存储
	rawRecord, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal event record: %w", err)
	}

	// 准备要插入的事件日志记录
	eventLog := &SQLiteEventLog{
		EventID:   record.EventID,
		EventTime: record.EventTime.Format(time.RFC3339),
		EventName: record.EventName,
		UserID:    record.UserIdentity.AccountID,
		RawRecord: string(rawRecord),
	}

	// 使用GORM插入记录
	db := s.db.WithContext(ctx)
	if err := db.Create(eventLog).Error; err != nil {
		return fmt.Errorf("failed to insert event log into SQLite: %w", err)
	}

	return nil
}

// Query 按条件查询事件日志
func (s *SQLiteTarget) Query(ctx context.Context, cond *QueryCondition, opts *QueryOption) (*QueryResult, error) {
	db := s.db.WithContext(ctx)

	// 构建查询
	query := db.Model(&SQLiteEventLog{})

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
			query = query.Where("user_id = ?", *cond.UserID)
		}
	}

	// 查询总记录数
	total := int64(0)
	if err := query.Count(&total).Error; err != nil {
		return nil, fmt.Errorf("failed to count event logs: %w", err)
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
	var logs []SQLiteEventLog
	if err := query.Order(orderBy).Limit(maxCount).Offset(offset).Find(&logs).Error; err != nil {
		return nil, fmt.Errorf("failed to query event logs: %w", err)
	}

	// 转换为Record对象
	records := make([]*event.Record, 0, len(logs))
	for _, log := range logs {
		// 从RawRecord解析回Record对象
		var record event.Record
		if err := json.Unmarshal([]byte(log.RawRecord), &record); err != nil {
			// 如果解析失败，跳过该条记录
			continue
		}
		records = append(records, &record)
	}

	// 判断是否还有更多数据
	hasMore := int64(len(logs)) == total || int64(len(logs)) >= int64(maxCount)

	return &QueryResult{
		Records: records,
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
