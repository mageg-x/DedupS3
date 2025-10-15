package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mageg-x/dedups3/plugs/audit"
	"path/filepath"
	"sync"
	"time"

	xconf "github.com/mageg-x/dedups3/internal/config"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/queue"
	"github.com/mageg-x/dedups3/plugs/audit/target"
)

type AuditService struct {
	queue  queue.Queue
	target target.AuditTarget
}

const (
	maxBytesPerFile = 256 << 20 // 256MB // 64MB
	minMsgSize      = 8
	maxMsgSize      = 1 << 20 // 1MB
	syncEvery       = 100     // 每条都 fsync（性能低但安全）
	syncTimeout     = 1 * time.Second
)

var (
	instance *AuditService
	mu       = sync.Mutex{}
)

func GetAuditService() *AuditService {
	mu.Lock()
	defer mu.Unlock()
	if instance != nil && instance.queue != nil {
		return instance
	}
	cfg := xconf.Get()

	// 创建磁盘队列
	dir := filepath.Join(cfg.Node.LocalDir, "queue")
	dq := queue.NewDiskQueue("audit-queue", dir, maxBytesPerFile, minMsgSize, maxMsgSize, syncEvery, syncTimeout)
	if dq == nil {
		logger.GetLogger("dedups3").Errorf("failed to create audit queue")
		return nil
	}

	// 创建target
	t, err := target.NewAudit(&target.Args{
		Driver:    cfg.Audit.Driver,
		DSN:       cfg.Audit.DSN,
		AuthToken: cfg.Audit.AuthToken,
	})
	if err != nil || t == nil {
		logger.GetLogger("dedups3").Errorf("failed to create audit target")
		_ = dq.Close()
		return nil
	}
	instance = &AuditService{
		queue:  dq,
		target: t,
	}

	instance.doSyncAudit(context.Background())
	return instance
}

func (a *AuditService) doSyncAudit(ctx context.Context) {
	go func() {
		// 从队列中读取消息
		readChan := a.queue.ReadChan()

		for {
			select {
			case <-ctx.Done(): // 可以响应取消信号
				logger.GetLogger("dedups3").Info("sync block stopping due to context cancellation")
				return
			case msg := <-readChan:
				recorder := audit.Entry{}
				err := json.Unmarshal(msg, &recorder)
				if err != nil {
					logger.GetLogger("dedups3").Errorf("failed to unmarshal audit record: %v", err)
					continue
				}
				if a.target != nil {
					if err := a.target.Send(ctx, &recorder); err != nil {
						logger.GetLogger("dedups3").Errorf("failed to send audit record: %v", err)
					}
				}
			default:
				// 正常的处理逻辑
			}
		}
	}()
}

func (a *AuditService) Send(data []byte) error {
	err := a.queue.Put(data)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to put audit.Entry to disk: %v", err)
		return fmt.Errorf("failed to put evaudit.Entry to disk: %w", err)
	}
	return nil
}

// Query 按条件查询审计日志
func (a *AuditService) Query(ctx context.Context, cond *target.QueryCondition, opts *target.QueryOption) (*target.QueryResult, error) {
	if a.target == nil {
		return &target.QueryResult{
			Entries: make([]*audit.Entry, 0),
			HasMore: false,
			Total:   0,
		}, nil
	}

	result, err := a.target.Query(ctx, cond, opts)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to query audit log: %v", err)
		return nil, fmt.Errorf("failed to query audit log: %w", err)
	}

	return result, nil
}
