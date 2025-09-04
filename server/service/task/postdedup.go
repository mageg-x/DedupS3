package task

import (
	"github.com/mageg-x/boulder/internal/logger"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mageg-x/boulder/internal/storage/kv"
)

type DupChunk struct {
	ChunkID  string
	BlockID1 string
	BlockID2 string
}

const (
	DefaultDedupScanInterval = 30 * time.Second
	DedupChunkPrefix         = "aws:dedup:chunk:"
)

var (
	dedupInst *PostDedupService
	dedupMu   = sync.Mutex{}
)

type PostDedupService struct {
	running atomic.Bool
	kvstore kv.KVStore
	mutex   sync.Mutex
}

// GetPostDedupService 获取全局GC服务实例
func GetPostDedupService() *PostDedupService {
	dedupMu.Lock()
	defer dedupMu.Unlock()
	if dedupInst != nil {
		return dedupInst
	}
	logger.GetLogger("boulder").Infof("initializing post-dedup clean service")
	kvStore, err := kv.GetKvStore()
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get kv store for task: %v", err)
		return nil
	}
	dedupInst = &PostDedupService{
		kvstore: kvStore,
		running: atomic.Bool{},
		mutex:   sync.Mutex{},
	}
	logger.GetLogger("boulder").Infof("post-dedup clean  service initialized successfully")

	return dedupInst
}

// Start 启动后置重删任务
func (g *PostDedupService) Start() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	if g.running.Load() {
		logger.GetLogger("boulder").Infof("post-dedup clean  service is already running")
		return nil
	}

	g.running.Store(true)

	go g.loop()
	logger.GetLogger("boulder").Infof("post-dedup clean  service started successfully")
	return nil
}

// Stop 停止后置重删任务
func (g *PostDedupService) Stop() {
	g.mutex.Lock()
	g.mutex.Unlock()
	g.running.Store(false)
	logger.GetLogger("boulder").Infof("post-dedup clean  service stopped successfully")
}

func (g *PostDedupService) loop() {
	for g.running.Load() {
		g.doDedup()
		time.Sleep(DefaultDedupScanInterval)
	}
}
func (g *PostDedupService) doDedup() {

}
