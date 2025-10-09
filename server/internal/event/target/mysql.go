package target

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"strings"
	"sync"
	"time"

	"github.com/mageg-x/dedups3/internal/utils"
	"gorm.io/gorm"
)

// EventModel 用于gorm存储事件数据的模型
// 优化的事件模型
type EventModel struct {
	ID        uint      `gorm:"primarykey;autoIncrement"`
	EventTime time.Time `gorm:"column:event_time;type:datetime(3);index;not null"`
	EventData string    `gorm:"column:event_data;type:json;not null"`
	CreatedAt time.Time `gorm:"column:created_at;type:datetime(3);not null;autoCreateTime"`
}

// TableName 表名方法
func (EventModel) TableName() string {
	// 注意：这里返回默认表名，实际表名由调用方决定
	return "event_models"
}

// MySQLArgs 包含MySQL目标的配置参数
type MySQLArgs struct {
	TargetArgHead
	DSN   string `json:"dsn"`   // 数据源名称，格式: user:password@tcp(host:port)/dbname
	Table string `json:"table"` // 存储事件的表名
}

// 添加连接状态管理
type mysqlConnectionState struct {
	db    *gorm.DB
	sqlDB *sql.DB
	mu    sync.RWMutex
}

// MySQLTarget 优化后的 MySQLTarget 结构
type MySQLTarget struct {
	initOnce   utils.Init
	args       MySQLArgs
	state      *mysqlConnectionState
	cancel     context.CancelFunc
	cancelCh   <-chan struct{}
	maxRetries int
	retryDelay time.Duration
	mu         sync.RWMutex
}

// NewMySQLTarget 创建新的MySQL目标
func NewMySQLTarget(ctx context.Context, args MySQLArgs) (*MySQLTarget, error) {
	if args.DSN == "" {
		return nil, errors.New("mysql dsn cannot be empty")
	}

	if args.Table == "" {
		args.Table = "minio_events"
	}

	_ctx, cancel := context.WithCancel(ctx)
	target := &MySQLTarget{
		args:     args,
		cancel:   cancel,
		cancelCh: _ctx.Done(),
	}

	return target, nil
}

// ID 返回目标ID
func (target *MySQLTarget) ID() string {
	return target.args.ID
}

func (target *MySQLTarget) Arn() string {
	return target.args.Arn
}

func (target *MySQLTarget) Owner() string {
	return target.args.Owner
}
func (target *MySQLTarget) Type() string {
	return EVENT_TARGET_TYPE_MYSQL
}

func (target *MySQLTarget) Init() error {
	return target.initOnce.Do(func() error {
		target.state = &mysqlConnectionState{}
		target.maxRetries = 3
		target.retryDelay = 2 * time.Second
		return target.connect()
	})
}

func (target *MySQLTarget) IsActive() (bool, error) {
	if err := target.Init(); err != nil {
		return false, err
	}

	target.state.mu.RLock()
	defer target.state.mu.RUnlock()

	if target.state.sqlDB == nil {
		return false, errors.New("mysql connection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 使用更轻量级的检查方法
	var result int
	err := target.state.db.WithContext(ctx).Raw("SELECT 1").Scan(&result).Error
	if err != nil {
		return false, fmt.Errorf("mysql health check failed: %w", err)
	}

	return result == 1, nil
}

func (target *MySQLTarget) Send(eventData Event) error {
	if err := target.Init(); err != nil {
		return err
	}

	select {
	case <-target.cancelCh:
		return errors.New("mysql target is closed")
	default:
	}

	// 序列化事件数据
	data, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	// 带重试的发送
	return target.sendWithRetry(data)
}

// Close 关闭MySQL连接
func (target *MySQLTarget) Close() error {
	target.cancel()

	target.mu.Lock()
	defer target.mu.Unlock()

	return target.cleanup()
}

// GetArg 返回MySQL目标的参数
func (target *MySQLTarget) GetArg() (interface{}, error) {
	return target.args, nil
}

func (target *MySQLTarget) connect() error {
	target.state.mu.Lock()
	defer target.state.mu.Unlock()

	// 清理旧连接
	_ = target.cleanup()

	// 配置 GORM
	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn), // 只记录警告和错误
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "",
			SingularTable: true, // 使用单数表名
		},
		PrepareStmt: true, // 开启预编译语句提升性能
	}

	var err error
	target.state.db, err = gorm.Open(mysql.Open(target.args.DSN), gormConfig)
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %w", err)
	}

	// 获取底层 SQL DB 并配置连接池
	target.state.sqlDB, err = target.state.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql db: %w", err)
	}

	// 优化连接池配置
	target.state.sqlDB.SetMaxOpenConns(25)
	target.state.sqlDB.SetMaxIdleConns(10)
	target.state.sqlDB.SetConnMaxLifetime(2 * time.Hour)
	target.state.sqlDB.SetConnMaxIdleTime(30 * time.Minute)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := target.state.sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("mysql ping failed: %w", err)
	}

	// 确保表存在
	if err := target.ensureTable(); err != nil {
		return err
	}

	return nil
}

func (target *MySQLTarget) ensureTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 使用更精确的表创建方式，避免不必要的迁移
	var tableExists int
	err := target.state.db.WithContext(ctx).Raw(`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`, target.args.Table).Scan(&tableExists).Error
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if tableExists == 0 {
		// 使用更优化的表结构
		err = target.state.db.WithContext(ctx).Table(target.args.Table).AutoMigrate(&EventModel{})
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		// 添加索引以提高查询性能
		err = target.state.db.WithContext(ctx).Exec(`ALTER TABLE ` + target.args.Table + ` ADD INDEX idx_event_time (event_time)`).Error
		if err != nil {
			// 索引创建失败不是致命错误，记录但继续
			fmt.Printf("Warning: failed to create index: %v\n", err)
		}
	}

	return nil
}

func (target *MySQLTarget) sendWithRetry(data []byte) error {
	var lastErr error

	for i := 0; i < target.maxRetries; i++ {
		select {
		case <-target.cancelCh:
			return errors.New("target closed during send")
		default:
		}

		if err := target.trySend(data); err != nil {
			lastErr = err

			// 如果是连接错误，尝试重连
			if isConnectionError(err) {
				if reconnectErr := target.reconnect(); reconnectErr != nil {
					lastErr = fmt.Errorf("send failed and reconnect failed: %w (original: %v)", reconnectErr, err)
				}
				// 等待后重试
				time.Sleep(target.retryDelay * time.Duration(i+1))
				continue
			}
			// 非连接错误直接返回
			return err
		}
		return nil
	}

	return fmt.Errorf("failed to send message after %d attempts: %w", target.maxRetries, lastErr)
}

func (target *MySQLTarget) trySend(data []byte) error {
	target.state.mu.RLock()
	defer target.state.mu.RUnlock()

	if target.state.db == nil {
		return errors.New("mysql connection not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eventModel := &EventModel{
		EventTime: time.Now().UTC(), // 使用 UTC 时间
		EventData: string(data),
	}

	// 使用更高效的插入方式
	result := target.state.db.WithContext(ctx).Table(target.args.Table).Create(eventModel)
	if result.Error != nil {
		return fmt.Errorf("insert failed: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		return errors.New("no rows affected by insert")
	}

	return nil
}

// 判断是否为连接错误
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := err.Error()
	connectionErrors := []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"driver: bad connection",
		"database is closed",
		"context deadline exceeded",
		"i/o timeout",
	}

	for _, connErr := range connectionErrors {
		if strings.Contains(strings.ToLower(errorStr), connErr) {
			return true
		}
	}

	return false
}

// 重连方法
func (target *MySQLTarget) reconnect() error {
	target.mu.Lock()
	defer target.mu.Unlock()

	return target.connect()
}

// BatchSend 添加批量插入支持
func (target *MySQLTarget) BatchSend(events []Event) error {
	if len(events) == 0 {
		return nil
	}

	if err := target.Init(); err != nil {
		return err
	}

	select {
	case <-target.cancelCh:
		return errors.New("mysql target is closed")
	default:
	}

	// 准备批量数据
	models := make([]*EventModel, len(events))
	now := time.Now().UTC()

	for i, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event data at index %d: %w", i, err)
		}

		models[i] = &EventModel{
			EventTime: now,
			EventData: string(data),
		}
	}

	// 批量插入
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result := target.state.db.WithContext(ctx).Table(target.args.Table).CreateInBatches(models, 100) // 每批100条
	if result.Error != nil {
		return fmt.Errorf("batch insert failed: %w", result.Error)
	}

	return nil
}

func (target *MySQLTarget) cleanup() error {
	if target.state == nil {
		return nil
	}

	target.state.mu.Lock()
	defer target.state.mu.Unlock()

	var errs []error

	if target.state.sqlDB != nil {
		if err := target.state.sqlDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("sql db close: %w", err))
		}
		target.state.sqlDB = nil
	}

	target.state.db = nil

	if len(errs) > 0 {
		return fmt.Errorf("errors during cleanup: %v", errs)
	}
	return nil
}
