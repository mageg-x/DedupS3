package target

import (
	"context"
	"fmt"
	"time"

	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/plugs/audit"
)

const (
	AuditTargetPrefix        = "aws:audit:target:"
	AUDIT_TARGET_TYPE_SQLITE = "sqlite-target"
)

// Args 审计目标创建参数
type Args struct {
	Driver    string `json:"driver"`               // "sqlite" 或 "http"
	DSN       string `json:"dsn"`                  // SQLite文件路径 或 HTTP服务URL
	AuthToken string `json:"auth_token,omitempty"` // HTTP服务的认证token
}

// QueryCondition 定义查询条件（可扩展）
type QueryCondition struct {
	StartTime *time.Time // 起始时间（含）
	EndTime   *time.Time // 结束时间（含）
	EventName *string    // 审计事件类型，如 "CreateUser"
	UserID    *string    // 用户名或 ARN
}

// QueryOption 查询选项（用于分页、排序等）
type QueryOption struct {
	Offset  int    //  起始偏移 ，默认 0
	OrderBy string // 如 "eventTime DESC"
}

// QueryResult 查询结果
type QueryResult struct {
	Entries []*audit.Entry
	HasMore bool  // 是否还有更多
	Total   int64 // 符合条件的总记录数（用于分页）
}

// Target 审计存储接口
type AuditTarget interface {
	// Send 写入单条审计日志
	Send(ctx context.Context, entry *audit.Entry) error
	// Query 按条件查询审计日志
	Query(ctx context.Context, cond *QueryCondition, opts *QueryOption) (*QueryResult, error)
	Close() error
}

// NewAudit 工厂函数，根据不同配置创建不同的审计目标
func NewAudit(args *Args) (AuditTarget, error) {
	if args == nil {
		return nil, fmt.Errorf("audit args cannot be nil")
	}

	logger.GetLogger("dedups3").Infof("Creating audit target of type %s", args.Driver)

	// 根据类型创建不同的审计目标
	switch args.Driver {
	case "sqlite":
		// 创建SQLite审计目标
		if args.DSN == "" {
			args.DSN = "./audit.db"
		}
		return NewSQLiteTarget(args)
	case "http":
		// 创建HTTP审计目标
		if args.DSN == "" {
			return nil, fmt.Errorf("http audit target requires DSN (service URL)")
		}
		return NewHTTPTarget(args.DSN, args.AuthToken)
	default:
		return nil, fmt.Errorf("unsupported audit target driver: %s", args.Driver)
	}
}
