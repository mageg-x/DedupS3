package chunk

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	xcache "github.com/mageg-x/boulder/internal/storage/cache"
	"github.com/mageg-x/boulder/service/gc"

	fastcdc "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
	"github.com/mageg-x/boulder/service/block"
)

var (
	instance *ChunkService
	mu       = sync.Mutex{}
)

type ChunkService struct {
	kvstore kv.KVStore
}
type WriteObjCB func(cs *ChunkService, chunks []*meta.Chunk, blocks map[string]*meta.Block, obj *meta.BaseObject) error

func GetChunkService() *ChunkService {
	mu.Lock()
	defer mu.Unlock()
	if instance != nil && instance.kvstore != nil {
		return instance
	}

	store, err := kv.GetKvStore()
	if err != nil || store == nil {
		logger.GetLogger("boulder").Errorf("failed to get kv store: %v", err)
		return nil
	}
	instance = &ChunkService{
		kvstore: store,
	}

	return instance
}

func (c *ChunkService) DoChunk(r io.Reader, obj *meta.BaseObject, cb WriteObjCB) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 创建输出通道
	chunkChan := make(chan *meta.Chunk, 100)

	// 配置分块器选项
	opts := &fastcdc.ChunkerOpts{
		MinSize:    1024 * 1024,
		NormalSize: 2 * 1024 * 1024,
		MaxSize:    4 * 1024 * 1024,
	}
	// 小文件分块切分粒度小一些
	if obj.Size < 16*1024*1024 && obj.ObjType != meta.PART_OBJECT {
		opts = &fastcdc.ChunkerOpts{
			MinSize:    16 * 1024,
			NormalSize: 128 * 1024,
			MaxSize:    512 * 1024,
		}
	}

	// 切分
	go func() {
		defer close(chunkChan)
		// 直接传递 r.Body (io.ReadCloser) 给期望 io.Reader 的函数
		err := c.Split(ctx, r, chunkChan, opts, obj)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s split chunk failed: %v", obj.Bucket, obj.Key, err)
			cancel()
			return
		}
		logger.GetLogger("boulder").Tracef("split %s/%schunk finished", obj.Bucket, obj.Key)
	}()

	// 去重
	allChan := make(chan []*meta.Chunk, 1)
	dedupChan := make(chan *meta.Chunk, 100)

	go func() {
		defer close(dedupChan)
		defer close(allChan)
		chunks, err := c.Dedup(ctx, chunkChan, dedupChan, obj)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s Dedup chunk failed: %v", obj.Bucket, obj.Key, err)
			cancel()
			return
		}

		allChan <- chunks
	}()

	rollback := false
	//重组
	blocks, err := c.Assemble(ctx, dedupChan, obj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s assemble chunk failed: %v", obj.Bucket, obj.Key, err)
		rollback = true
		cancel()
	}

	// 汇总信息
	allChunk, err := c.Summary(ctx, allChan, blocks, obj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s summary chunk failed: %v", obj.Bucket, obj.Key, err)
		rollback = true
		cancel()
	} else {
		logger.GetLogger("boulder").Debugf("%s/%s summary chunk finished %+v", obj.Bucket, obj.Key, allChunk)
	}

	if !rollback {
		// 开始写入 元数据
		txErr := cb(c, allChunk, blocks, obj)
		if txErr != nil {
			rollback = true
			cancel()
		}
	}

	// 回滚处理，删除之前写入的block
	if rollback {
		if len(blocks) > 0 {
			gcKey := gc.GCBlockPrefix + utils.GenUUID()
			gcData := gc.GCBlock{
				GCData: gc.GCData{
					CreateAt: time.Now().UTC(),
					Items:    make([]gc.GCItem, 0),
				},
			}
			for _, _block := range blocks {
				gcData.Items = append(gcData.Items, gc.GCItem{StorageID: obj.DataLocation, ID: _block.ID})
			}
			err := c.kvstore.Set(gcKey, &gcData)
			if err != nil {
				logger.GetLogger("boulder").Errorf("%s/%s save blocks failed: %v", obj.Bucket, obj.Key, err)
			}
		}

		return fmt.Errorf("dochunk %s/%s failed, then rollback %d block data", obj.Bucket, obj.Key, len(blocks))
	}

	return nil
}

// Split DoChunk 简单的CDC分块函数
func (c *ChunkService) Split(ctx context.Context, r io.Reader, outputChan chan *meta.Chunk, opt *fastcdc.ChunkerOpts, obj *meta.BaseObject) error {
	// bodyData, err := io.ReadAll(r)
	// 把 []byte 转成 io.Reader
	// reader := bytes.NewReader(bodyData)
	// 创建CDC分块器
	chunker, err := fastcdc.NewChunker("fastcdc", r, opt)
	if err != nil {
		return fmt.Errorf("error creating chunker: %w", err)
	}
	var prevChunk *meta.Chunk

	//hashfile, _ := os.Create(fmt.Sprintf("%s.hash", obj.Key))
	//defer hashfile.Close()
	//hashfile, _ := os.OpenFile("split.chunkid", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	//defer hashfile.Close()

	objSize := 0 // 统计真实长度
	// 循环读取并分块
	for {
		// 检查 context 是否已取消
		select {
		case <-ctx.Done():
			return fmt.Errorf("fast cdc %s/%s be canceled: %w", obj.Bucket, obj.Key, ctx.Err())
		default:
			// 继续执行
		}

		chunkData, err := chunker.Next()
		if err != nil && err != io.EOF {
			return fmt.Errorf("cdc chunk %s/%s error: %w", obj.Bucket, obj.Key, err)
		}

		objSize += len(chunkData)

		// 如果有数据，发送到输出通道
		if len(chunkData) > 0 {
			currentChunk := meta.NewChunk(chunkData)
			//fmt.Fprintf(hashfile, "%x\n", currentChunk.Hash)
			// 避免最后一个分片太小
			if err == io.EOF && len(chunkData) < opt.MinSize && prevChunk != nil {
				// 合并到前一个分片
				mergedData := append(prevChunk.Data, chunkData...)
				prevChunk = meta.NewChunk(mergedData)
			} else {
				// 发送前一个分片（如果有）
				if prevChunk != nil {
					//hashfile.WriteString(fmt.Sprintf("%s  %s\n", obj.Key, prevChunk.Hash))
					outputChan <- prevChunk
				}
				// 当前分片成为新的前一个分片
				prevChunk = currentChunk
			}
		}

		// 检查是否结束
		if err == io.EOF {
			break
		}
	}
	// 发送最后一个分片（可能是合并后的）
	if prevChunk != nil {
		//hashfile.WriteString(fmt.Sprintf("%s  %s\n", obj.Key, prevChunk.Hash))
		outputChan <- prevChunk
	}
	logger.GetLogger("boulder").Infof("split object %s/%s  to chunk finished, get size %d:%d", obj.Bucket, obj.Key, objSize, obj.Size)
	obj.Size = int64(objSize)
	return nil
}

func (c *ChunkService) Dedup(ctx context.Context, inputChan, dedupChan chan *meta.Chunk, obj *meta.BaseObject) ([]*meta.Chunk, error) {
	allChunk := make([]*meta.Chunk, 0)
	chunkFilter := make(map[string]string)
	// 初始化 MD5 哈希用于计算整个数据块的MD5
	fullMD5 := md5.New()
	// 批量处理
	batchDedup := make([]*meta.Chunk, 0)
	finished := false
	dedupNum := 0
	offset := 0
	for !finished {
		select {
		case <-ctx.Done():
			// 超时或取消，退出循环
			logger.GetLogger("boulder").Warnf("chunking timed out or cancelled: %v", ctx.Err())
			return nil, fmt.Errorf("chunking timed out or cancelled for %s/%s: %w", obj.Bucket, obj.Key, ctx.Err())
		case chunk, ok := <-inputChan:
			if ok && chunk != nil {
				//logger.GetLogger("boulder").Debugf("get cdc chunk %d fp %s", chunk.Size, chunk.Hash)
				// 更新整个数据块的 MD5
				fullMD5.Write(chunk.Data)
				allChunk = append(allChunk, chunk)
				batchDedup = append(batchDedup, chunk)
			} else {
				// 通道已关闭，退出循环
				finished = true
			}

			if len(batchDedup) > 100 || (finished && len(batchDedup) > 0) {
				keys := make([]string, 0)
				for _, item := range batchDedup {
					chunkKey := meta.GenChunkKey(obj.DataLocation, item.Hash)
					keys = append(keys, chunkKey)
				}

				_chunks, err := c.kvstore.BatchGet(keys)
				if err != nil {
					logger.GetLogger("boulder").Errorf("%s/%s batch get failed: %v", obj.Bucket, obj.Key, err)
					return nil, fmt.Errorf("%s/%s batch get failed: %w", obj.Bucket, obj.Key, err)
				}

				for _, item := range batchDedup {
					offset += int(item.Size)
					// 对象间去重
					chunkKey := meta.GenChunkKey(obj.DataLocation, item.Hash)
					if _chunks[chunkKey] != nil {
						var _chunk meta.Chunk
						err = json.Unmarshal(_chunks[chunkKey], &_chunk)
						if err != nil {
							logger.GetLogger("boulder").Errorf("%s/%s chunk unmarshal failed: %v", obj.Bucket, obj.Key, err)
							return nil, fmt.Errorf("%s/%s chunk unmarshal failed: %w", obj.Bucket, obj.Key, err)
						}

						chunkFilter[item.Hash] = _chunk.BlockID
						logger.GetLogger("boulder").Debugf("chunk %s/%s/%s [%d-%d] has already been dedupped in block %s between object",
							obj.Bucket, obj.Key, item.Hash, offset-int(item.Size), offset, _chunk.BlockID)
						item.BlockID = _chunk.BlockID
						item.Data = nil
						dedupNum++
						continue
					}

					// 对象内去重
					if chunkFilter[item.Hash] != "" {
						logger.GetLogger("boulder").Infof("chunk %s/%s/%s [%d-%d] has already been dedupped inner object", obj.Bucket, obj.Key, item.Hash, offset-int(item.Size), offset)
						item.BlockID = chunkFilter[item.Hash]
						item.Data = nil
						dedupNum++
						continue
					}

					logger.GetLogger("boulder").Debugf("chunk %s/%s/%s has not found dedupped", obj.Bucket, obj.Key, item.Hash)
					chunkFilter[item.Hash] = meta.NONE_BLOCK_ID

					dedupChan <- item
				}
				batchDedup = make([]*meta.Chunk, 0)
			}
		}
	}

	// 计算整个数据块的 MD5
	fullMD5Sum := fullMD5.Sum(nil)
	fullMD5Hex := hex.EncodeToString(fullMD5Sum)
	if string(obj.ETag) != "" && string(obj.ETag) != fullMD5Hex {
		// 服务器计算出出来的md5 和 客户端上传的Content-MD5 不一致
		return nil, fmt.Errorf("Content-MD5 mismatch for %s/%s: %s:%s", obj.Bucket, obj.Key, obj.ETag, fullMD5Hex)
	}
	obj.ETag = meta.Etag(fullMD5Hex)
	logger.GetLogger("boulder").Infof("dedump object %s/%s finished, md5 is %s  all chunk num is %d dedup chunk num is %d", obj.Bucket, obj.Key, fullMD5Hex, len(allChunk), dedupNum)
	return allChunk, nil
}

func (c *ChunkService) Assemble(ctx context.Context, dedupChan chan *meta.Chunk, obj *meta.BaseObject) (map[string]*meta.Block, error) {
	blocks := make(map[string]*meta.Block)
	finished := false
	blockNum := 0
	for !finished {
		select {
		case <-ctx.Done():
			logger.GetLogger("boulder").Warnf(" assemble chunk be cancelled: %v", ctx.Err())
			return blocks, fmt.Errorf("assemble chunk be cancelled for %s/%s: %w", obj.Bucket, obj.Key, ctx.Err())
		case chunk, ok := <-dedupChan:
			bs := block.GetBlockService()
			if bs == nil {
				logger.GetLogger("boulder").Errorf("%s/%s block service is nil", obj.Bucket, obj.Key)
				return blocks, fmt.Errorf("%s/%s block service is nil", obj.Bucket, obj.Key)
			}

			_block, err := bs.PutChunk(chunk, obj)
			if err != nil {
				logger.GetLogger("boulder").Errorf("%s/%s put chunk failed: %v", obj.Bucket, obj.Key, err)
				return blocks, fmt.Errorf("%s/%s put chunk failed: %w", obj.Bucket, obj.Key, err)
			}

			if _block != nil {
				blocks[_block.ID] = _block
				blockNum = len(blocks)
			}

			if !ok || chunk == nil {
				finished = true
				logger.GetLogger("boulder").Infof("block service is finish assemble object %s/%s", obj.Bucket, obj.Key)
				break
			}
		}
	}
	logger.GetLogger("boulder").Infof("assemble object %s/%s finished, get block num %d", obj.Bucket, obj.Key, blockNum)
	return blocks, nil
}

// Summary 主要用途是 修正 chunk 里面的blockid 索引信息, 统计 object 的实际大小
func (c *ChunkService) Summary(ctx context.Context, allChan chan []*meta.Chunk, blocks map[string]*meta.Block, obj *meta.BaseObject) ([]*meta.Chunk, error) {
	allChunks := make([]*meta.Chunk, 0)
	chunk2block := make(map[string]string)
	for _, _block := range blocks {
		for _, _chunk := range _block.ChunkList {
			chunk2block[_chunk.Hash] = _block.ID
			//logger.GetLogger("boulder").Debugf(" chunk ----> block : %s--->%s", _chunk.Hash, _block.ID)
		}
	}

	select {
	case <-ctx.Done():
		logger.GetLogger("boulder").Errorf("%s/%s do chunk be canceled", obj.Bucket, obj.Key)
		return allChunks, fmt.Errorf("%s/%s do chunk be canceled", obj.Bucket, obj.Key)
	case result := <-allChan:
		if result == nil {
			logger.GetLogger("boulder").Errorf("%s/%s summary chunk failed", obj.Bucket, obj.Key)
			return allChunks, fmt.Errorf("%s/%s summary chunk failed", obj.Bucket, obj.Key)
		}
		for _, chunk := range result {
			if chunk == nil {
				logger.GetLogger("boulder").Errorf("%s/%s summary chunk failed", obj.Bucket, obj.Key)
				return allChunks, fmt.Errorf("%s/%s summary chunk failed", obj.Bucket, obj.Key)
			}
			//logger.GetLogger("boulder").Debugf("chunk %s block id %s:%s", chunk.Hash, chunk.BlockID, chunk2block[chunk.Hash])
			if chunk2block[chunk.Hash] != "" {
				chunk.BlockID = chunk2block[chunk.Hash]
			}

			if chunk.BlockID == "" {
				logger.GetLogger("boulder").Errorf("%s/%s/%s summary chunk failed", obj.Bucket, obj.Key, chunk.Hash)
				return allChunks, fmt.Errorf("%s/%s summary chunk failed", obj.Bucket, obj.Key)
			}
			chunk.Data = nil
			allChunks = append(allChunks, chunk)
		}
		break
	}

	logger.GetLogger("boulder").Infof("object summary %s/%s finished", obj.Bucket, obj.Key)
	return allChunks, nil
}

func (c *ChunkService) WriteMeta(ctx context.Context, accountID string, allChunk []*meta.Chunk, blocks map[string]*meta.Block, object interface{}, objPrefix string) error {
	objType := reflect.TypeOf(object)
	logger.GetLogger("boulder").Infof("write meta object type %s", objType.String())
	var normalobj *meta.Object
	var partobj *meta.PartObject
	var obj *meta.BaseObject

	oldChunkKeys := make([]string, 0)
	oldBlockKeys := make([]string, 0)

	defer func() {
		if cache, err := xcache.GetCache(); err == nil && cache != nil {
			_ = cache.MDel(ctx, oldChunkKeys)
			_ = cache.MDel(ctx, oldBlockKeys)
		}
	}()
	// 根据类型做不同处理
	switch v := object.(type) {
	case *meta.Object:
		logger.GetLogger("boulder").Infof("object is *meta.Object")
		normalobj = v
		obj = meta.ObjectToBaseObject(normalobj)
	case *meta.PartObject:
		logger.GetLogger("boulder").Infof("object is *meta.PartObject")
		partobj = v
		obj = meta.PartToBaseObject(partobj)
	default:
		fmt.Printf("object is unkown type: %s", objType.String())
		return fmt.Errorf("object is unkown type: %s", objType.String())
	}

	txn, err := c.kvstore.BeginTxn(ctx, nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s create transaction failed: %v", obj.Bucket, obj.Key, err)
		return fmt.Errorf("%s/%s create transaction failed: %w", obj.Bucket, obj.Key, err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	type Pair struct {
		BlockID string
		ChunkID string
	}

	chunk2block := make(map[string]string, 0)

	// 写入chunk元数据
	for _, chunk := range allChunk {
		//logger.GetLogger("boulder").Errorf("%s/%s write chunk %s:%s:%d ", obj.Bucket, obj.Key, chunk.Hash, chunk.BlockID, len(chunk.Data))
		chunkey := meta.GenChunkKey(obj.DataLocation, chunk.Hash)
		var _old_chunk meta.Chunk
		exists, e := txn.Get(chunkey, &_old_chunk)
		if e != nil {
			logger.GetLogger("boulder").Errorf("%s/%s get chunk failed: %v", obj.Bucket, obj.Key, err)
			return fmt.Errorf("%s/%s get chunk %s failed: %w", obj.Bucket, obj.Key, chunkey, err)
		}

		if exists {
			if _old_chunk.BlockID != chunk.BlockID {
				logger.GetLogger("boulder").Debugf("%s/%s  chunk %s has multi bolock %s:%s", obj.Bucket, obj.Key, chunk.Hash, _old_chunk.BlockID, chunk.BlockID)
			}

			// 一个chunk只能属于一个 block
			//chunk.BlockID = _old_chunk.BlockID
			chunk2block[chunk.Hash] = _old_chunk.BlockID

			oldBlockKeys = append(oldBlockKeys, meta.GenBlockKey(obj.DataLocation, _old_chunk.BlockID))

			_old_chunk.RefCount += 1
			e := txn.Set(chunkey, &_old_chunk)
			if e != nil {
				logger.GetLogger("boulder").Errorf("%s/%s set chunk %s failed: %v", obj.Bucket, obj.Key, _old_chunk.Hash, err)
				return fmt.Errorf("%s/%s set chunk failed: %w", obj.Bucket, obj.Key, err)
			} else {
				logger.GetLogger("boulder").Debugf("%s/%s refresh set chunk: %s to block %s:%s", obj.Bucket, obj.Key, chunk.Hash, chunk.BlockID, _old_chunk.BlockID)
			}
		} else {
			chunk.RefCount = 1
			e := txn.Set(chunkey, &chunk)
			if e != nil {
				logger.GetLogger("boulder").Errorf("%s/%s set chunk %s failed: %v", obj.Bucket, obj.Key, chunkey, err)
				return fmt.Errorf("%s/%s set chunk failed: %w", obj.Bucket, obj.Key, err)
			} else {
				chunk2block[chunk.Hash] = chunk.BlockID
				logger.GetLogger("boulder").Debugf("%s/%s refresh set chunk: %s to block %s", obj.Bucket, obj.Key, chunk.Hash, chunk.BlockID)
			}
		}
	}

	// 写入block的元数据
	for _, item := range blocks {
		var _oldBlock meta.Block
		blockKey := meta.GenBlockKey(obj.DataLocation, item.ID)
		exists, err := txn.Get(blockKey, &_oldBlock)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s get block %s failed: %v", obj.Bucket, obj.Key, blockKey, err)
			return fmt.Errorf("%s/%s get block %s failed: %w", obj.Bucket, obj.Key, blockKey, err)
		}
		_curBlock := item
		if exists {
			if len(_oldBlock.ChunkList) > len(item.ChunkList) {
				_curBlock = &_oldBlock
			} else {
				for i := 0; i < len(_oldBlock.ChunkList); i++ {
					_curBlock.ChunkList[i] = _oldBlock.ChunkList[i]
				}
			}
		}
		for i := 0; i < len(_curBlock.ChunkList); i++ {
			bid, ok := chunk2block[_curBlock.ChunkList[i].Hash]
			if ok && bid != _curBlock.ID {
				_curBlock.ChunkList[i].Hash = meta.NONE_CHUNK_ID // 无索引的chunk
			}
		}
		//logger.GetLogger("boulder").Errorf("%s/%s write block meta inf : %s:%d:%d", obj.Bucket, obj.Key, item.ID, item.TotalSize, item.RealSize)

		err = txn.Set(blockKey, _curBlock)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s/%s set block meta failed: %v", obj.Bucket, obj.Key, _curBlock.ID, err)
			return fmt.Errorf("%s/%s set block meta failed: %w", obj.Bucket, obj.Key, err)
		} else {
			logger.GetLogger("boulder").Debugf("%s/%s/%s set block meta   ok", obj.Bucket, obj.Key, _curBlock.ID)
		}
	}

	//写入object meta信息
	objKey := objPrefix + accountID + ":" + obj.Bucket + "/" + obj.Key
	obj.Chunks = make([]string, 0, len(allChunk))
	for _, _chunk := range allChunk {
		obj.Chunks = append(obj.Chunks, _chunk.Hash)
	}
	logger.GetLogger("boulder").Infof("prepare to write object %s  meta ...", obj.Key)
	// 如果是覆盖，需要先删除旧的索引
	gcChunks := make([]string, 0)
	switch object.(type) {
	case *meta.PartObject:
		var _obj meta.PartObject
		exists, err := txn.Get(objKey, &_obj)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s get object failed: %v", obj.Bucket, obj.Key, err)
			return fmt.Errorf("%s/%s get object failed: %w", obj.Bucket, obj.Key, err)
		}
		if exists && len(_obj.Chunks) > 0 {
			gcChunks = append(gcChunks, _obj.Chunks...)
			for _, k := range _obj.Chunks {
				oldChunkKeys = append(oldChunkKeys, meta.GenChunkKey(_obj.DataLocation, k))
			}
		}
	case *meta.Object:
		var _obj meta.Object
		exists, err := txn.Get(objKey, &_obj)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s get object failed: %v", obj.Bucket, obj.Key, err)
			return fmt.Errorf("%s/%s get object failed: %w", obj.Bucket, obj.Key, err)
		}
		if exists && len(_obj.Chunks) > 0 {
			gcChunks = append(gcChunks, _obj.Chunks...)
			for _, k := range _obj.Chunks {
				oldChunkKeys = append(oldChunkKeys, meta.GenChunkKey(_obj.DataLocation, k))
			}
		}
	default:
		logger.GetLogger("boulder").Errorf("unsupport type %s", objType.String())
		return fmt.Errorf("unsupport type %s", objType.String())
	}

	if len(gcChunks) > 0 {
		gckey := gc.GCChunkPrefix + utils.GenUUID()
		gcData := gc.GCChunk{
			GCData: gc.GCData{
				CreateAt: time.Now().UTC(),
				Items:    make([]gc.GCItem, 0),
			},
		}
		for _, id := range gcChunks {
			gcData.Items = append(gcData.Items, gc.GCItem{StorageID: obj.DataLocation, ID: id})
		}

		err = txn.Set(gckey, &gcData)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s set gc chunk failed: %v", obj.Bucket, obj.Key, err)
			return fmt.Errorf("%s/%s set gc chunk failed: %w", obj.Bucket, obj.Key, err)
		} else {
			logger.GetLogger("boulder").Infof("%s/%s set gc chunk %s delay to proccess", obj.Bucket, obj.Key, gckey)
		}
	}

	if len(blocks) > 0 {
		// 后置重删检查
		gcKey := gc.GCDedupPrefix + utils.GenUUID()
		gcData := gc.GCDedup{
			GCData: gc.GCData{
				CreateAt: time.Now().UTC(),
				Items:    make([]gc.GCItem, 0),
			},
		}

		for _, _block := range blocks {
			gcData.Items = append(gcData.Items, gc.GCItem{StorageID: _block.StorageID, ID: _block.ID})
		}

		err = txn.Set(gcKey, &gcData)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s set post dedup block failed: %v", obj.Bucket, obj.Key, err)
		} else {
			logger.GetLogger("boulder").Infof("%s/%s set gc dedup block delay to proccess", obj.Bucket, obj.Key)
		}
	}

	logger.GetLogger("boulder").Debugf("set object %s etag %s , put meta %#v ..... ", objKey, obj.ETag, obj.Size)
	if normalobj != nil {
		err = txn.Set(objKey, normalobj)
	} else if partobj != nil {
		err = txn.Set(objKey, partobj)
	}

	if err != nil {
		logger.GetLogger("boulder").Errorf("set object %s/%s meta info failed: %v", obj.Bucket, obj.Key, err)
		return fmt.Errorf("set object %s/%s meta info failed: %w", obj.Bucket, obj.Key, err)
	} else {
		logger.GetLogger("boulder").Debugf("set object %s/%s meta  ok", obj.Bucket, obj.Key)
	}

	err = txn.Commit()
	if err != nil {
		logger.GetLogger("boulder").Debugf("%s/%s commit failed: %v", obj.Bucket, obj.Key, err)
		return kv.ErrTxnCommit
	}
	txn = nil

	logger.GetLogger("boulder").Infof("write object %s/%s  all meta data finish", obj.Bucket, obj.Key)
	return nil
}

func (c *ChunkService) BatchGet(storageID string, chunkIDs []string) ([]*meta.Chunk, error) {
	chunkMap := make(map[string]*meta.Chunk)
	keys := make([]string, 0, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		chunkKey := meta.GenChunkKey(storageID, chunkID)
		keys = append(keys, chunkKey)
	}

	batchSize := 100
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		batchKeys := keys[i:end]
		if cache, err := xcache.GetCache(); err == nil && cache != nil {
			result, err := cache.MGet(context.Background(), batchKeys)
			if err == nil {
				for k, item := range result {
					chunk := item.(*meta.Chunk)
					if chunk != nil {
						chunkMap[chunk.Hash] = chunk
					} else {
						_ = cache.Del(context.Background(), k)
					}
				}
			}
		}

		newBatch := make([]string, 0, len(batchKeys))
		for _, key := range batchKeys {
			chunkID := key[len("aws:chunk:"+storageID+":"):]
			_, ok := chunkMap[chunkID]
			if !ok {
				newBatch = append(newBatch, key)
			}
		}
		batchKeys = newBatch

		result, err := c.kvstore.BatchGet(batchKeys)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to batchGet chunks: %v", err)
			return nil, fmt.Errorf("failed to batchGet chunks: %w", err)
		}
		for k, v := range result {
			var chunk meta.Chunk
			err := json.Unmarshal(v, &chunk)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to Unmarshal chunks %s err: %v", k, err)
				return nil, fmt.Errorf("failed to Unmarshal chunks %s err: %w", k, err)
			}
			chunkMap[chunk.Hash] = &chunk

			if cache, err := xcache.GetCache(); err == nil && cache != nil {
				chunkKey := meta.GenChunkKey(storageID, chunk.Hash)
				err := cache.Set(context.Background(), chunkKey, &chunk, time.Second*600)
				if err != nil {
					logger.GetLogger("boulder").Errorf("set chunk %s to cache failed: %v", chunkKey, err)
				}
			}
		}
	}

	chunks := make([]*meta.Chunk, 0, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		chunk, ok := chunkMap[chunkID]
		if !ok {
			logger.GetLogger("boulder").Errorf("chunk %s not exist", chunkID)
			return nil, fmt.Errorf("chunk %s not exist", chunkID)
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}
