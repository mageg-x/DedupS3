package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mageg-x/dedups3/plugs/kv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mageg-x/dedups3/internal/queue"
	"github.com/mageg-x/dedups3/internal/utils"

	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/meta"
)

const (
	STATS_PREKEY = "aws:stats:"
)

var (
	instance *StatsService
	mu       sync.Mutex
)

type StatsOfGlobal struct {
	UpdateAt time.Time `json:"updateAt"`
	// 统计全局
	AccountCount int64 `json:"accountCount"`
	BucketCount  int64 `json:"bucketCount"`
	ObjectCount  int64 `json:"objectCount"`
	ChunkCount   int64 `json:"chunkCount"`
	BlockCount   int64 `json:"blockCount"`
	ObjectSize   int64 `json:"objectSize"`
	ChunkSize    int64 `json:"chunkSize"`
	BlockSize1   int64 `json:"blockSize1"` //没有压缩后根据block 统计的大小
	BlockSize2   int64 `json:"blockSize2"` //压缩后的block 统计大小
}

type StatsOfAccount struct {
	UpdateAt time.Time `json:"updateAt"`
	// 统计Account
	BucketCount       int64 `json:"bucketCount"`
	ObjectCount       int64 `json:"objectCount"`
	BlockCount        int64 `json:"blockCount"`
	ChunkCount        int64 `json:"chunkCount"`
	ChunkCountOfDedup int64 `json:"chunkCountOfDedup"` // 去重后的chunk数量
	SizeOfObject      int64 `json:"sizeOfObject"`      // 根据object 统计的数据大小，=== 原始数据大小
	SizeOfChunk       int64 `json:"sizeOfChunk"`       // 根据chunk统计 ==  去重后的数据大小
	SizeofBlock1      int64 `json:"sizeofBlock1"`      // 没有压缩后根据block 统计的大小  ==== 近似 去重后的数据大小
	SizeOfBlock2      int64 `json:"sizeOfBlock2"`      // 压缩后的block 统计大小
}

type StatsOfBucket struct {
	UpdateAt          time.Time `json:"updateAt"`
	ObjectCount       int64     `json:"objectCount"`
	BlockCount        int64     `json:"blockCount"`
	ChunkCount        int64     `json:"chunkCount"`
	ChunkCountOfDedup int64     `json:"chunkCountOfDedup"`
	SizeOfObject      int64     `json:"sizeOfObject"`
	SizeOfChunk       int64     `json:"sizeOfChunk"`
}

type Stats struct {
	AccountStats        *StatsOfAccount `json:"accountStats"`
	LastMonAccountStats *StatsOfAccount `json:"lastMonAccountStats,omitempty"`
	GlobalStats         *StatsOfGlobal  `json:"globalStats"`
	LastGlobalStats     *StatsOfGlobal  `json:"lastGlobalStats,omitempty"`
}

type StatsService struct {
	running atomic.Bool
	kvstore kv.KVStore
	mutex   sync.Mutex
	taskQ   *queue.MQueue
}

func GetStatsService() *StatsService {
	mu.Lock()
	defer mu.Unlock()
	if instance != nil {
		return instance
	}

	_kv, err := kv.GetKvStore()
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Error getting kv store for stats service: %v", err)
		return nil
	}

	instance = &StatsService{
		kvstore: _kv,
		running: atomic.Bool{},
		mutex:   sync.Mutex{},
		taskQ:   queue.NewMergeQueue(nil),
	}
	logger.GetLogger("dedups3").Info("Starting stats service")
	return instance
}

// Start 启动垃圾回收服务
func (s *StatsService) Start() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.running.Load() {
		logger.GetLogger("dedups3").Infof("stats service is already running")
		return nil
	}

	s.running.Store(true)

	go s.loop()
	logger.GetLogger("dedups3").Infof("stats service started successfully")
	return nil
}

// Stop 停止垃圾回收服务
func (s *StatsService) Stop() {
	s.mutex.Lock()
	s.mutex.Unlock()
	s.running.Store(false)
	logger.GetLogger("dedups3").Infof("stats service stopped successfully")
}

func (s *StatsService) GetBucketStats(bucketID string) (*StatsOfBucket, error) {
	bKey := STATS_PREKEY + "bucket:" + bucketID + ":" + time.Now().Format("200601")
	today_stats := &StatsOfBucket{}
	if exists, err := s.kvstore.Get(bKey, today_stats); err != nil {
		return nil, err
	} else if !exists {
		return nil, fmt.Errorf("stats key %s does not found", bKey)
	}
	return today_stats, nil
}

func (s *StatsService) GetAccountStats(accountID string) (*Stats, error) {
	// 获取已有的数据
	uKey := STATS_PREKEY + "account:" + accountID + ":" + time.Now().Format("200601")
	today_stats := &StatsOfAccount{}
	if exists, err := s.kvstore.Get(uKey, today_stats); err != nil {
		return nil, err
	} else if !exists {
		s.RefreshAccountStats(accountID)
		return nil, fmt.Errorf("stats key %s does not exist", uKey)
	}

	// 获取上个月的数据
	uKey = STATS_PREKEY + "account:" + accountID + ":" + time.Now().AddDate(0, -1, 0).Format("200601")
	premon_stats := &StatsOfAccount{}
	if exists, err := s.kvstore.Get(uKey, premon_stats); err != nil || !exists {
		premon_stats = nil
	}

	gKey := STATS_PREKEY + "global" + ":" + time.Now().Format("200601")
	g_stats := &StatsOfGlobal{}
	if exists, err := s.kvstore.Get(gKey, g_stats); err != nil {
		return nil, err
	} else if !exists {
		s.RefreshAccountStats("global")
		return nil, fmt.Errorf("stats key %s does not exist", gKey)
	}

	// 获取上个月的数据
	gKey = STATS_PREKEY + "global" + ":" + time.Now().AddDate(0, -1, 0).Format("200601")
	g_premon_stats := &StatsOfGlobal{}
	if exists, err := s.kvstore.Get(gKey, g_premon_stats); err != nil || exists {
		g_premon_stats = nil
	}

	return &Stats{AccountStats: today_stats, LastMonAccountStats: premon_stats, GlobalStats: g_stats, LastGlobalStats: g_premon_stats}, nil
}

func (s *StatsService) RefreshAccountStats(accountID string) {
	s.taskQ.Push(&queue.MQItem{Key: accountID, Value: nil})
}

func (s *StatsService) loop() {
	// 每小时触发一次的 ticker
	hourlyTicker := time.NewTicker(1 * time.Hour)
	defer hourlyTicker.Stop()
	// 首次立即执行一次（可选）
	_ = s.doStats4Global()

	for s.running.Load() {
		item := s.taskQ.Pop()
		if item != nil {
			switch item.Key {
			case "global":
				_ = s.doStats4Global()
			default:
				_ = s.doStats4Account(item.Key)
			}
		}

		// 检查是否到整点（每小时执行一次）
		select {
		case <-hourlyTicker.C:
			_ = s.doStats4Global()
		default:
		}

		time.Sleep(time.Second)
	}
}

func (s *StatsService) doStats4Account(accountID string) error {
	logger.GetLogger("dedups3").Errorf("doStats4Account %s", accountID)
	// 避免多个并发统计同一accountID
	lockKey := "aws:lock:stats:" + accountID
	if ok, _ := s.kvstore.TryLock(lockKey, "StatsService", time.Hour); !ok {
		return fmt.Errorf("stats service is locked by other routine")
	}
	defer s.kvstore.UnLock(lockKey, "StatsService")

	txn, err := s.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}

	defer txn.Rollback()
	// 同时顺带统计桶的数据
	bucketOfAccount := make(map[string]*StatsOfBucket, 0)

	// 统计每个 account下的 bucket数量
	myBuckets := make([]string, 0, 100)
	myStats := &StatsOfAccount{}
	{
		// 统计桶数量
		bkPrefix := "aws:bucket:" + accountID + ":"
		nk := ""
		for {
			bucketKeys, nk, err := txn.Scan(bkPrefix, nk, 1000)
			if err != nil {
				logger.GetLogger("dedups3").Errorf("failed to scan buckets for account %s: %v", accountID, err)
				return fmt.Errorf("failed to scan buckets for account %s: %w", accountID, err)
			}
			for _, bkID := range bucketKeys {
				bucketID := bkID[len(bkPrefix):]
				myBuckets = append(myBuckets, bucketID)
			}
			if nk == "" {
				break
			}
		}
	}
	myStats.BucketCount += int64(len(myBuckets))
	logger.GetLogger("dedups3").Tracef("get %d buckets for account %s", len(myBuckets), accountID)

	// 统计每个 account下的 object 数量
	type statsObject struct {
		objectKey string
		bucketID  string
	}
	myObjects := make([]*statsObject, 0, 100)
	for _, bucketID := range myBuckets {
		_bucketStats := &StatsOfBucket{}
		objPrefix := "aws:object:" + accountID + ":" + bucketID + "/"
		nk := ""
		for {
			objKeys, nk, err := txn.Scan(objPrefix, nk, 1000)
			if err != nil {
				logger.GetLogger("dedups3").Errorf("failed to scan objects for account %s bucket %s: %v", accountID, bucketID, err)
				return fmt.Errorf("failed to scan objects for account %s bucket %s: %w", accountID, bucketID, err)
			}
			for _, objKey := range objKeys {
				myObjects = append(myObjects, &statsObject{objectKey: objKey, bucketID: bucketID})
				_bucketStats.ObjectCount++
			}
			if nk == "" {
				break
			}
		}
		bucketOfAccount[bucketID] = _bucketStats
	}
	myStats.ObjectCount += int64(len(myObjects))
	logger.GetLogger("dedups3").Tracef("get %d objects for account %s", len(myObjects), accountID)

	// 批量获取object 信息， 统计 chunk 数量
	type statsChunk struct {
		chunkID   string
		bucketID  string
		storageID string
	}
	myChunks := make([]*statsChunk, 0)
	const batchSize = 100 // 每批次处理的对象数量
	// 分批处理对象
	for i := 0; i < len(myObjects); i += batchSize {
		end := i + batchSize
		if end > len(myObjects) {
			end = len(myObjects)
		}

		// 获取当前批次的键
		batchObjs := myObjects[i:end]
		objectMap := make(map[string]string)
		batchKeys := make([]string, 0, len(batchObjs))
		for _, o := range batchObjs {
			batchKeys = append(batchKeys, o.objectKey)
			objectMap[o.objectKey] = o.bucketID
		}
		// 批量获取对象信息
		results, err := txn.BatchGet(batchKeys)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to batch get objects for account %s: %v", accountID, err)
			return fmt.Errorf("failed to batch get objects for account %s: %w", accountID, err)
		}

		// 处理每个对象的信息
		for key, data := range results {
			var obj meta.Object
			if err := json.Unmarshal(data, &obj); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal object %s: %v", key, err)
				continue
			}

			bucketID := objectMap[key]
			if bucketID != "" && bucketOfAccount[bucketID] != nil {
				bucketStats := bucketOfAccount[bucketID]
				bucketStats.SizeOfObject += obj.Size
				bucketStats.ChunkCount += int64(len(obj.Chunks))
			}

			myStats.ChunkCount += int64(len(obj.Chunks))
			myStats.SizeOfObject += obj.Size
			for _, chunk := range obj.Chunks {
				myChunks = append(myChunks, &statsChunk{chunkID: chunk, bucketID: obj.Bucket, storageID: obj.DataLocation})
			}
		}
	}
	logger.GetLogger("dedups3").Tracef("get %d chunks for account %s", len(myChunks), accountID)

	// 统计去重后的chunk数量
	dedupChunkMap := make(map[string]*statsChunk)
	dedupChunks := make([]*statsChunk, 0, 100)
	for _, _chunk := range myChunks {
		if dedupChunkMap[_chunk.chunkID] == nil {
			dedupChunks = append(dedupChunks, _chunk)
			dedupChunkMap[_chunk.chunkID] = _chunk
		}
	}
	myStats.ChunkCountOfDedup = int64(len(dedupChunks))

	// 根据chunkID 批量获取 chunk meta，计算block个数
	type statsBlock struct {
		blockID   string
		storageID string
	}
	blockMap := make(map[string]bool, 0)
	myBlocks := make([]statsBlock, 0, 100)
	SizeOfChunk := int64(0)
	// 分批处理chunk
	const chunkBatchSize = 100
	for i := 0; i < len(dedupChunks); i += chunkBatchSize {
		end := i + chunkBatchSize
		if end > len(dedupChunks) {
			end = len(dedupChunks)
		}
		batchChunks := dedupChunks[i:end]

		// 构建批量获取的键
		chunkKeys := make([]string, 0, len(batchChunks))
		chunkMap := make(map[string]*statsChunk, 0) // 保存键和原始chunk信息的映射
		for _, _chunk := range batchChunks {
			chunkKey := meta.GenChunkKey(_chunk.storageID, _chunk.chunkID)
			chunkKeys = append(chunkKeys, chunkKey)
			chunkMap[chunkKey] = _chunk
		}

		// 批量获取chunk信息
		chunkResults, err := txn.BatchGet(chunkKeys)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to batch get chunks for account %s: %v", accountID, err)
			return fmt.Errorf("failed to batch get chunks for account %s: %w", accountID, err)
		}

		// 处理获取到的chunk信息
		for chunkKey, chunkData := range chunkResults {
			var chunk meta.Chunk
			if err := json.Unmarshal(chunkData, &chunk); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal chunk %s: %v", chunkKey, err)
				continue
			}

			SizeOfChunk += int64(chunk.Size)
			_chunk := chunkMap[chunkKey]
			if !blockMap[chunk.BlockID] {
				blockMap[chunk.BlockID] = true
				myBlocks = append(myBlocks, statsBlock{blockID: chunk.BlockID, storageID: _chunk.storageID})
			}
			if _cs := dedupChunkMap[chunk.Hash]; _cs != nil {
				if bucketStats := bucketOfAccount[_cs.bucketID]; bucketStats != nil {
					bucketStats.ChunkCountOfDedup++
					bucketStats.SizeOfChunk += int64(chunk.Size)
					if blockMap[chunk.BlockID] {
						bucketStats.BlockCount++
					}
				}
			}
		}
	}
	myStats.BlockCount = int64(len(myBlocks))
	myStats.SizeOfChunk = SizeOfChunk

	// 根据blockid批量获取block meta信息，计算realsize
	const blockBatchSize = 100
	SizeOfBlock1 := int64(0)
	SizeOfBlock2 := int64(0)
	// 分批处理block
	for i := 0; i < len(myBlocks); i += blockBatchSize {
		end := i + blockBatchSize
		if end > len(myBlocks) {
			end = len(myBlocks)
		}
		batchBlocks := myBlocks[i:end]

		// 构建批量获取的键
		blockKeys := make([]string, 0, len(batchBlocks))
		for _, block := range batchBlocks {
			blockKey := meta.GenBlockKey(block.storageID, block.blockID)
			blockKeys = append(blockKeys, blockKey)
		}

		// 批量获取block信息
		blockResults, err := txn.BatchGet(blockKeys)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to batch get blocks for account %s: %v", accountID, err)
			return fmt.Errorf("failed to batch get blocks for account %s: %w", accountID, err)
		}

		// 处理获取到的block信息
		for _, blockData := range blockResults {
			var block meta.Block
			if err := json.Unmarshal(blockData, &block); err != nil {
				continue
			}
			logger.GetLogger("dedups3").Errorf("block info %#v", block)
			SizeOfBlock1 += block.TotalSize
			SizeOfBlock2 += block.RealSize
		}
	}
	myStats.SizeofBlock1 = SizeOfBlock1
	myStats.SizeOfBlock2 = SizeOfBlock2

	// 把结果写入kv 存储
	errMsg := utils.RetryCall(10, func() error {
		// 开始事务
		_txn, _err := s.kvstore.BeginTxn(context.Background(), nil)
		if _err != nil {
			logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", _err)
			return fmt.Errorf("failed to initialize kvstore txn: %w", _err)
		}
		defer func() {
			if _txn != nil {
				_ = _txn.Rollback()
			}
		}()

		accountStatsKey := STATS_PREKEY + "account:" + accountID + ":" + time.Now().Format("200601")
		myStats.UpdateAt = time.Now().UTC()
		if _err := _txn.Set(accountStatsKey, myStats); _err != nil {
			logger.GetLogger("dedups3").Errorf("failed to set stats for account %s: %v", accountID, _err)
			return fmt.Errorf("failed to set stats for account %s: %w", accountID, _err)
		}

		for bucketID, bucketStats := range bucketOfAccount {
			bucketStatsKey := STATS_PREKEY + "bucket:" + bucketID + ":" + time.Now().Format("200601")
			bucketStats.UpdateAt = time.Now().UTC()
			if _err := _txn.Set(bucketStatsKey, bucketStats); _err != nil {
				logger.GetLogger("dedups3").Errorf("failed to set stats for bucket %s: %v", bucketID, _err)
				return fmt.Errorf("failed to set stats for bucket %s: %w", bucketID, _err)
			}
		}
		// 提交事务
		if _err := _txn.Commit(); _err != nil {
			logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", _err)
			return fmt.Errorf("failed to commit transaction: %w", _err)
		}
		_txn = nil
		return nil
	})

	if errMsg != nil {
		return errMsg
	}
	jsonAccountStats, _ := json.Marshal(myStats)
	jsonBucketStats, _ := json.Marshal(bucketOfAccount)
	logger.GetLogger("dedups3").Errorf("account %s  stats %s bucket stats %s", accountID, string(jsonAccountStats), string(jsonBucketStats))
	return nil
}

func (s *StatsService) doStats4Global() error {
	// 避免多个并发统计同一accountID
	lockKey := "aws:lock:stats:global"
	if ok, err := s.kvstore.TryLock(lockKey, "StatsService", time.Hour); !ok {
		return fmt.Errorf("stats service is locked by other routine %w", err)
	}
	defer s.kvstore.UnLock(lockKey, "StatsService")

	txn, err := s.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}

	defer txn.Rollback()

	globalStats := &StatsOfGlobal{}
	// 遍历统计 全局account
	allAccounts := make([]string, 0, 100)
	{
		acPrefix := "dedups3:default:iam-account:"
		nk := ""
		for {
			acs, nk, err := txn.Scan(acPrefix, nk, 1000)
			if err != nil {
				logger.GetLogger("dedups3").Errorf("failed to scan more accounts: %v", err)
				return fmt.Errorf("failed to scan more accounts: %w", err)
			}
			// 收集所有账户ID
			for _, ac := range acs {
				accountID := ac[len(acPrefix):]
				allAccounts = append(allAccounts, accountID)
			}
			if nk == "" {
				break
			}
		}

		logger.GetLogger("dedups3").Tracef("get %d account", len(allAccounts))
	}
	globalStats.AccountCount = int64(len(allAccounts))

	// 遍历统计bucket 数量
	allBuckets := make(map[string]bool, 0)
	{
		bucketPrefix := "aws:bucket:"
		nk := ""
		for {
			bucketKeys, nextKey, err := txn.Scan(bucketPrefix, nk, 1000)
			if err != nil {
				logger.GetLogger("dedups3").Errorf("failed to scan buckets: %v", err)
				return fmt.Errorf("failed to scan buckets: %w", err)
			}
			for _, bucketKey := range bucketKeys {
				allBuckets[bucketKey] = true
			}
			nk = nextKey
			if nk == "" {
				break
			}
		}
		globalStats.BucketCount = int64(len(allBuckets))
		logger.GetLogger("dedups3").Tracef("global bucket count: %d", globalStats.BucketCount)
	}

	// 遍历统计object数量和大小
	allObjects := make(map[string]bool, 0)
	{
		objectPrefix := "aws:object:"
		nk := ""
		const batchSize = 100 // 每批次处理的object数量
		batchObjectKeys := make([]string, 0, batchSize)

		for {
			objectKeys, nextKey, err := txn.Scan(objectPrefix, nk, 1000)
			if err != nil {
				logger.GetLogger("dedups3").Errorf("failed to scan objects: %v", err)
				return fmt.Errorf("failed to scan objects: %w", err)
			}

			// 收集object键并批量处理
			for _, objectKey := range objectKeys {
				allObjects[objectKey] = true
				batchObjectKeys = append(batchObjectKeys, objectKey)

				// 达到批次大小时处理
				if len(batchObjectKeys) >= batchSize {
					// 批量获取object信息
					results, err := txn.BatchGet(batchObjectKeys)
					if err != nil {
						logger.GetLogger("dedups3").Errorf("failed to batch get objects: %v", err)
					} else {
						for _, data := range results {
							var object meta.Object
							if err := json.Unmarshal(data, &object); err == nil {
								globalStats.ObjectSize += object.Size
							}
						}
					}
					// 清空批次
					batchObjectKeys = batchObjectKeys[:0]
				}
			}

			// 处理剩余的object
			if len(batchObjectKeys) > 0 {
				results, err := txn.BatchGet(batchObjectKeys)
				if err == nil {
					for _, data := range results {
						var object meta.Object
						if err := json.Unmarshal(data, &object); err == nil {
							globalStats.ObjectSize += object.Size
						}
					}
				}
				batchObjectKeys = batchObjectKeys[:0]
			}

			nk = nextKey
			if nk == "" {
				break
			}
		}
		globalStats.ObjectCount = int64(len(allObjects))
		logger.GetLogger("dedups3").Tracef("global object count: %d, size: %d", globalStats.ObjectCount, globalStats.ObjectSize)
	}

	// 遍历统计chunk数量和大小
	allChunks := make(map[string]bool, 0)
	{
		chunkPrefix := "aws:chunk:"
		nk := ""
		const batchSize = 100 // 每批次处理的chunk数量
		batchChunkKeys := make([]string, 0, batchSize)

		for {
			chunkKeys, nextKey, err := txn.Scan(chunkPrefix, nk, 1000)
			if err != nil {
				logger.GetLogger("dedups3").Errorf("failed to scan chunks: %v", err)
				return fmt.Errorf("failed to scan chunks: %w", err)
			}

			// 收集chunk键并批量处理
			for _, chunkKey := range chunkKeys {
				allChunks[chunkKey] = true
				batchChunkKeys = append(batchChunkKeys, chunkKey)

				// 达到批次大小时处理
				if len(batchChunkKeys) >= batchSize {
					// 批量获取chunk信息
					results, err := txn.BatchGet(batchChunkKeys)
					if err != nil {
						logger.GetLogger("dedups3").Errorf("failed to batch get chunks: %v", err)
					} else {
						for _, data := range results {
							var chunk meta.Chunk
							if err := json.Unmarshal(data, &chunk); err == nil {
								globalStats.ChunkSize += int64(chunk.Size)
							}
						}
					}
					// 清空批次
					batchChunkKeys = batchChunkKeys[:0]
				}
			}

			// 处理剩余的chunk
			if len(batchChunkKeys) > 0 {
				results, err := txn.BatchGet(batchChunkKeys)
				if err == nil {
					for _, data := range results {
						var chunk meta.Chunk
						if err := json.Unmarshal(data, &chunk); err == nil {
							globalStats.ChunkSize += int64(chunk.Size)
						}
					}
				}
				batchChunkKeys = batchChunkKeys[:0]
			}

			nk = nextKey
			if nk == "" {
				break
			}
		}
		globalStats.ChunkCount = int64(len(allChunks))
		logger.GetLogger("dedups3").Tracef("global chunk count: %d, size: %d", globalStats.ChunkCount, globalStats.ChunkSize)
	}

	// 遍历统计 block数量
	allBlocks := make(map[string]bool, 0)
	{
		blockPrefix := "aws:block:"
		nk := ""
		const batchSize = 100 // 每批次处理的block数量
		batchBlockKeys := make([]string, 0, batchSize)

		for {
			blockKeys, nextKey, err := txn.Scan(blockPrefix, nk, 1000)
			if err != nil {
				logger.GetLogger("dedups3").Errorf("failed to scan blocks: %v", err)
				return fmt.Errorf("failed to scan blocks: %w", err)
			}

			// 收集block键并批量处理
			for _, blockKey := range blockKeys {
				allBlocks[blockKey] = true
				batchBlockKeys = append(batchBlockKeys, blockKey)

				// 达到批次大小时处理
				if len(batchBlockKeys) >= batchSize {
					// 批量获取block信息
					results, err := txn.BatchGet(batchBlockKeys)
					if err != nil {
						logger.GetLogger("dedups3").Errorf("failed to batch get blocks: %v", err)
					} else {
						for _, data := range results {
							var block meta.Block
							if err := json.Unmarshal(data, &block); err == nil {
								globalStats.BlockSize1 += block.TotalSize
								globalStats.BlockSize2 += block.RealSize
							}
						}
					}
					// 清空批次
					batchBlockKeys = batchBlockKeys[:0]
				}
			}

			// 处理剩余的block
			if len(batchBlockKeys) > 0 {
				results, err := txn.BatchGet(batchBlockKeys)
				if err == nil {
					for _, data := range results {
						var block meta.Block
						if err := json.Unmarshal(data, &block); err == nil {
							globalStats.BlockSize1 += block.TotalSize
							globalStats.BlockSize2 += block.RealSize
						}
					}
				}
				batchBlockKeys = batchBlockKeys[:0]
			}

			nk = nextKey
			if nk == "" {
				break
			}
		}
		globalStats.BlockCount = int64(len(allBlocks))
		logger.GetLogger("dedups3").Tracef("global block count: %d, size: %d, %d", globalStats.BlockCount, globalStats.BlockSize1, globalStats.BlockSize2)
	}

	// 写入全局统计数据
	errMsg := utils.RetryCall(10, func() error {
		// 开始事务
		_txn, _err := s.kvstore.BeginTxn(context.Background(), nil)
		if _err != nil {
			logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", _err)
			return fmt.Errorf("failed to initialize kvstore txn: %w", _err)
		}
		defer func() {
			if _txn != nil {
				_ = _txn.Rollback()
			}
		}()

		globalStatsKey := STATS_PREKEY + "global" + ":" + time.Now().Format("200601")
		globalStats.UpdateAt = time.Now().UTC()
		if _err := _txn.Set(globalStatsKey, globalStats); _err != nil {
			logger.GetLogger("dedups3").Errorf("failed to set global stats: %v", _err)
			return fmt.Errorf("failed to set global stats: %w", _err)
		}
		// 提交事务
		if _err := _txn.Commit(); _err != nil {
			logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", _err)
			return fmt.Errorf("failed to commit transaction: %w", _err)
		}
		_txn = nil
		return nil
	})

	if errMsg != nil {
		return errMsg
	}
	jsonBytes, _ := json.Marshal(globalStats)
	logger.GetLogger("dedups3").Debugf("global stats: %s", string(jsonBytes))
	return nil
}
