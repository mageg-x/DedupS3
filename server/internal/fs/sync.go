package fs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mageg-x/boulder/internal/logger"
)

type SyncTarget interface {
	BlockPath(blockID string) string
	WriteBlockDirect(ctx context.Context, blockID string, data []byte) error
}

// SyncRequest 同步请求
type SyncRequest struct {
	Region   *FileRegion
	Priority int // 优先级：0=低，1=中，2=高
	Callback func(error)
}

// SyncManager 同步管理器
type SyncManager struct {
	mu           sync.RWMutex
	queues       [3]chan *SyncRequest // 按优先级分队列
	flushFunc    func(*FileRegion) error
	batchSize    int
	flushTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

// NewSyncManager 创建同步管理器
func NewSyncManager(flushFunc func(*FileRegion) error) *SyncManager {
	ctx, cancel := context.WithCancel(context.Background())

	sm := &SyncManager{
		queues: [3]chan *SyncRequest{
			make(chan *SyncRequest, 100), // 低优先级
			make(chan *SyncRequest, 200), // 中优先级
			make(chan *SyncRequest, 50),  // 高优先级
		},
		flushFunc:    flushFunc,
		batchSize:    10,
		flushTimeout: 100 * time.Millisecond,
		ctx:          ctx,
		cancel:       cancel,
	}

	// 启动工作协程
	sm.wg.Add(1)
	go sm.worker()

	return sm
}

// Submit 提交同步请求
func (sm *SyncManager) Submit(region *FileRegion, priority int, callback func(error)) error {
	sm.mu.RLock()
	if sm.closed {
		sm.mu.RUnlock()
		return ErrSystemClosed
	}
	sm.mu.RUnlock()

	if priority < 0 || priority >= len(sm.queues) {
		priority = 0
	}

	req := &SyncRequest{
		Region:   region,
		Priority: priority,
		Callback: callback,
	}

	select {
	case sm.queues[priority] <- req:
		return nil
	case <-sm.ctx.Done():
		return ErrSystemClosed
	default:
		// 队列满，降级处理
		if priority > 0 {
			return sm.Submit(region, priority-1, callback)
		}
		// 同步执行
		err := sm.flushFunc(region)
		if callback != nil {
			callback(err)
		}
		return err
	}
}

// worker 工作协程
func (sm *SyncManager) worker() {
	defer sm.wg.Done()

	ticker := time.NewTicker(sm.flushTimeout)
	defer ticker.Stop()

	batch := make([]*SyncRequest, 0, sm.batchSize)

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		for _, req := range batch {
			err := sm.flushFunc(req.Region)
			if req.Callback != nil {
				req.Callback(err)
			}
			if err != nil {
				logger.GetLogger("boulder").Errorf("flush failed for %s: %v", req.Region.Path, err)
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-sm.ctx.Done():
			flushBatch()
			return

		case <-ticker.C:
			flushBatch()

		case req := <-sm.queues[2]: // 高优先级
			batch = append(batch, req)
			if len(batch) >= sm.batchSize {
				flushBatch()
			}

		case req := <-sm.queues[1]: // 中优先级
			batch = append(batch, req)
			if len(batch) >= sm.batchSize {
				flushBatch()
			}

		case req := <-sm.queues[0]: // 低优先级
			batch = append(batch, req)
			if len(batch) >= sm.batchSize {
				flushBatch()
			}
		}
	}
}

// Close 关闭同步管理器
func (sm *SyncManager) Close() error {
	sm.mu.Lock()
	if sm.closed {
		sm.mu.Unlock()
		return nil
	}
	sm.closed = true
	sm.mu.Unlock()

	sm.cancel()

	// 等待工作协程完成，带超时
	done := make(chan struct{})
	go func() {
		sm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("sync manager close timeout")
	}
}
