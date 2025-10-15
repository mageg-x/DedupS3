package event

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	xconf "github.com/mageg-x/dedups3/internal/config"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/queue"
	"github.com/mageg-x/dedups3/plugs/event"
	"github.com/mageg-x/dedups3/plugs/event/target"
)

const (
	maxBytesPerFile = 256 << 20 // 256MB // 64MB
	minMsgSize      = 8
	maxMsgSize      = 1 << 20 // 1MB
	syncEvery       = 100     // 每条都 fsync（性能低但安全）
	syncTimeout     = 1 * time.Second
)

type EventService struct {
	queue  queue.Queue
	target target.EventTarget
}

var (
	instance *EventService
	mu       = sync.Mutex{}
)

func GetEventService() *EventService {
	mu.Lock()
	defer mu.Unlock()
	if instance != nil && instance.queue != nil {
		return instance
	}
	cfg := xconf.Get()

	// 创建磁盘队列
	dir := filepath.Join(cfg.Node.LocalDir, "queue")
	dq := queue.NewDiskQueue("event-queue", dir, maxBytesPerFile, minMsgSize, maxMsgSize, syncEvery, syncTimeout)
	if dq == nil {
		logger.GetLogger("dedups3").Errorf("failed to create event queue")
		return nil
	}
	// 创建target
	t, err := target.NewEventTarget(&target.Args{
		Driver:    cfg.Event.Driver,
		DSN:       cfg.Event.DSN,
		AuthToken: cfg.Event.AuthToken,
	})
	if err != nil || t == nil {
		logger.GetLogger("dedups3").Errorf("failed to create event target")
		_ = dq.Close()
		return nil
	}

	instance = &EventService{
		queue:  dq,
		target: t,
	}

	instance.doSyncEvent(context.Background())
	return instance
}

func (e *EventService) doSyncEvent(ctx context.Context) {
	go func() {
		// 从队列中读取消息
		readChan := e.queue.ReadChan()

		for {
			select {
			case <-ctx.Done(): // 可以响应取消信号
				logger.GetLogger("dedups3").Info("sync block stopping due to context cancellation")
				return
			case msg := <-readChan:
				record := event.Record{}
				err := json.Unmarshal(msg, &record)
				if err != nil {
					logger.GetLogger("dedups3").Errorf("failed to unmarshal event: %v", err)
					continue
				}

				// 将事件记录发送到target存储
				if e.target != nil {
					err = e.target.Send(ctx, &record)
					if err != nil {
						logger.GetLogger("dedups3").Errorf("failed to send event to target: %v", err)
						// 可以考虑重试逻辑
						continue
					}
				}
			default:
				// 添加适当的延迟避免CPU占用过高
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
}

func (e *EventService) Send(data []byte) error {
	if e.queue == nil {
		return fmt.Errorf("event queue has not been initialized")
	}

	err := e.queue.Put(data)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to put event.Record to disk: %v", err)
		return fmt.Errorf("failed to put event.Record to disk: %w", err)
	}
	return nil
}

// Query 按条件查询审计日志
func (e *EventService) Query(ctx context.Context, cond *target.QueryCondition, opts *target.QueryOption) (*target.QueryResult, error) {
	if e.target == nil {
		return &target.QueryResult{
			Records: make([]*event.Record, 0),
			HasMore: false,
			Total:   0,
		}, nil
	}

	result, err := e.target.Query(ctx, cond, opts)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to query event log: %v", err)
		return nil, fmt.Errorf("failed to query event log: %w", err)
	}

	return result, nil
}
