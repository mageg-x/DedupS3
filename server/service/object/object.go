package object

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/service/block"
	"github.com/mageg-x/boulder/service/chunk"
	"github.com/mageg-x/boulder/service/task"
	"io"
	"net/http"
	"sync"
	"time"

	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	xcache "github.com/mageg-x/boulder/internal/storage/cache"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/meta"
	"github.com/mageg-x/boulder/service/iam"
	"github.com/mageg-x/boulder/service/storage"
)

var (
	instance *ObjectService
	mu       = sync.Mutex{}
)

type ObjectService struct {
	kvstore kv.KVStore
}

type BaseObjectParams struct {
	BucketName   string
	ObjKey       string
	AccessKeyID  string
	StorageClass string
	StorageID    string
	ContentLen   int64
	ContentType  string
	Range        *xhttp.HTTPRangeSpec
}

// ListObjectsResponse 对应 S3 ListObjects V1 响应
type ListObjectsResponse struct {
	XMLName xml.Name `xml:"ListBucketResult" json:"-"`
	// 命名空间属性
	XMLNS string `xml:"xmlns,attr,omitempty"`
	Name  string `xml:"Name"` // Bucket 名称

	// 可选字段（omitempty 控制：nil 或零值时不输出）
	Prefix       *string `xml:"Prefix,omitempty"`
	Marker       *string `xml:"Marker,omitempty"`
	MaxKeys      *int    `xml:"MaxKeys,omitempty"`
	Delimiter    *string `xml:"Delimiter,omitempty"`
	IsTruncated  *bool   `xml:"IsTruncated,omitempty"`
	NextMarker   *string `xml:"NextMarker,omitempty"`
	EncodingType *string `xml:"EncodingType,omitempty"`

	// Contents 列表（对象条目）
	Contents []ObjectContent `xml:"Contents,omitempty"`
}

// ObjectContent 表示一个对象条目
type ObjectContent struct {
	Key               string         `xml:"Key"`
	LastModified      time.Time      `xml:"LastModified"`
	ETag              string         `xml:"ETag,omitempty"`
	Size              int64          `xml:"Size,omitempty"`
	StorageClass      string         `xml:"StorageClass,omitempty"`
	Owner             *meta.Owner    `xml:"Owner,omitempty"`
	RestoreStatus     *RestoreStatus `xml:"RestoreStatus,omitempty"`
	ChecksumAlgorithm *string        `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType      *string        `xml:"ChecksumType,omitempty"`
}

// RestoreStatus 恢复状态（用于 Glacier/Deep Archive）
type RestoreStatus struct {
	IsRestoreInProgress *bool      `xml:"IsRestoreInProgress,omitempty"`
	RestoreExpiryDate   *time.Time `xml:"RestoreExpiryDate,omitempty"`
}

// CommonPrefix 表示一个公共前缀（如目录）
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func GetObjectService() *ObjectService {
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
	instance = &ObjectService{
		kvstore: store,
	}
	return instance
}

func (o *ObjectService) HeadObject(params *BaseObjectParams) (*meta.Object, error) {
	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		return nil, errors.New("failed to get iam service")
	}

	ak, err := iamService.GetAccessKey(params.AccessKeyID)
	if err != nil || ak == nil {
		logger.GetLogger("boulder").Errorf("failed to get access key %s", params.AccessKeyID)
		return nil, xhttp.ToError(xhttp.ErrAccessDenied)
	}

	// 检查object 是否存在
	objkey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
	var object *meta.Object
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		data, ok, e := cache.Get(context.Background(), objkey)
		if e == nil && ok {
			_object, yes := data.(*meta.Object)
			if yes {
				object = _object
			} else {
				// 缓存的数据类型错误，删除缓存
			}
		}
	}
	if object == nil {
		var _object meta.Object
		exist, err := o.kvstore.Get(objkey, &_object)
		if !exist || err != nil {
			logger.GetLogger("boulder").Errorf("object %s does not exist", objkey)
			return nil, xhttp.ToError(xhttp.ErrNoSuchKey)
		}
		object = &_object
		if cache, e := xcache.GetCache(); e == nil && cache != nil {
			cache.Set(context.Background(), objkey, object, time.Second*600)
		}
	}

	return object, nil
}

func (o *ObjectService) PutObject(r io.Reader, headers http.Header, params *BaseObjectParams) (*meta.Object, error) {
	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		return nil, errors.New("failed to get iam service")
	}

	ak, err := iamService.GetAccessKey(params.AccessKeyID)
	if err != nil || ak == nil {
		logger.GetLogger("boulder").Errorf("failed to get access key %s", params.AccessKeyID)
		return nil, xhttp.ToError(xhttp.ErrAccessDenied)
	}

	// 检查bucket是否存在
	key := "aws:bucket:" + ak.AccountID + ":" + params.BucketName
	var bucket *meta.BucketMetadata
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		data, ok, e := cache.Get(context.Background(), key)
		if e == nil && ok {
			_bucket, yes := data.(*meta.BucketMetadata)
			if yes {
				bucket = _bucket
			}
		}
	}
	if bucket == nil {
		var _bucket meta.BucketMetadata
		exist, err := o.kvstore.Get(key, &_bucket)
		if !exist || err != nil {
			logger.GetLogger("boulder").Errorf("bucket %s does not exist", params.BucketName)
			return nil, xhttp.ToError(xhttp.ErrNoSuchBucket)
		}
		bucket = &_bucket
		// 写入cache
		if cache, e := xcache.GetCache(); e == nil && cache != nil {
			cache.Set(context.Background(), key, bucket, time.Second*600)
		}
	}

	// 寻找合适的后端存储点
	storageClass := params.StorageClass
	if storageClass == "" {
		storageClass = "STANDARD"
	}

	bs := storage.GetStorageService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get storage service")
		return nil, fmt.Errorf("failed to get storage service")
	}

	scs := bs.GetStoragesByClass(storageClass)
	if len(scs) == 0 {
		logger.GetLogger("boulder").Errorf("no storage class %s", storageClass)
		return nil, fmt.Errorf("no storage class %s", storageClass)
	}
	sc := scs[0]

	objectInfo := meta.NewObject(params.BucketName, params.ObjKey)
	objectInfo.ParseHeaders(headers)
	objectInfo.StorageClass = storageClass
	objectInfo.DataLocation = sc.ID
	objectInfo.ContentType = params.ContentType
	objectInfo.Size = params.ContentLen
	objectInfo.Owner = meta.Owner{
		ID:          ak.AccountID,
		DisplayName: ak.Username,
	}
	objectInfo.LastModified = time.Now()
	logger.GetLogger("boulder").Debugf("put object %#v", objectInfo)

	// 进行chunk切分
	chunker := chunk.GetChunkService()
	if chunker == nil {
		logger.GetLogger("boulder").Errorf("failed to get chunk service")
		return nil, fmt.Errorf("failed to get chunk service")
	}

	defer func() {
		chunkKey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
		if cache, err := xcache.GetCache(); err == nil && cache != nil {
			cache.Del(context.Background(), chunkKey)
		}
	}()

	// 短body， 直接存放到元数据里面
	if params.ContentLen < 8*1024 {
		// 先压缩，如果压缩后小于 1024，就放到元数据里面，否则就跳过
		bodyBytes, err := io.ReadAll(r)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to read request body %v", err)
			return nil, fmt.Errorf("failed to read request body %v", err)
		}
		if len(bodyBytes) <= 1024 {
			objectInfo.ChunksInline = &meta.InlineChunk{
				Compress: false,
				Data:     bodyBytes,
			}
		} else {
			compress, err := utils.Compress(bodyBytes)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to compress request body %v", err)
				return nil, fmt.Errorf("failed to compress request body %v", err)
			}
			logger.GetLogger("boulder").Infof("object %s/%s data compress size %d/%d", objectInfo.Bucket, objectInfo.Key, len(compress), len(bodyBytes))
			if len(compress) <= 1024 {
				objectInfo.ChunksInline = &meta.InlineChunk{
					Compress: true,
					Data:     compress,
				}
			}
		}

		if objectInfo.ChunksInline != nil {
			// 计算etag
			hash := md5.Sum(bodyBytes)
			objectInfo.ETag = hex.EncodeToString(hash[:])
			objectInfo.Size = int64(len(bodyBytes))
			// 直接写meta
			err = chunker.WriteMeta(context.Background(), ak.AccountID, nil, nil, objectInfo)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to write %s/%s object inline chunk %v", objectInfo.Bucket, objectInfo.Key, err)
				return nil, fmt.Errorf("failed to write %s/%s object inline chunk %v", objectInfo.Bucket, objectInfo.Key, err)
			}
			return objectInfo, nil
		} else {
			// 但 r 已经 EOF！必须重建 reader
			r = bytes.NewReader(bodyBytes) // 从内存重建
		}
	}

	err = chunker.DoChunk(r, objectInfo)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to chunk object: %v", err)
	}

	return objectInfo, err
}

func (o *ObjectService) GetObject(r io.Reader, headers http.Header, params *BaseObjectParams) (*meta.Object, io.ReadCloser, error) {
	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		return nil, nil, errors.New("failed to get iam service")
	}

	ak, err := iamService.GetAccessKey(params.AccessKeyID)
	if err != nil || ak == nil {
		logger.GetLogger("boulder").Errorf("failed to get access key %s", params.AccessKeyID)
		return nil, nil, xhttp.ToError(xhttp.ErrAccessDenied)
	}

	// 检查object 是否存在
	objkey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
	var object *meta.Object
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		data, ok, e := cache.Get(context.Background(), objkey)
		if e == nil && ok {
			_object, yes := data.(*meta.Object)
			if yes {
				object = _object
			} else {
				// 缓存的数据类型错误，删除缓存
				cache.Del(context.Background(), objkey)
			}
		}
	}
	if object == nil {
		var _object meta.Object
		exist, err := o.kvstore.Get(objkey, &_object)
		if !exist || err != nil {
			logger.GetLogger("boulder").Errorf("object %s does not exist", objkey)
			return nil, nil, xhttp.ToError(xhttp.ErrNoSuchKey)
		}
		object = &_object
		if cache, e := xcache.GetCache(); e == nil && cache != nil {
			cache.Set(context.Background(), objkey, object, time.Second*600)
		}
	}

	// 计算数据范围
	start := int64(0)
	end := object.Size - 1
	if params.Range != nil {
		s, l, err := params.Range.GetOffsetLength(object.Size)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get the range of object %s , err %v", objkey, err)
			return nil, nil, err
		}
		start = s
		end = s + l - 1
	}
	logger.GetLogger("boulder").Infof("read object %s meta %#v", objkey, object.ETag)
	// 数据内联
	if object.ChunksInline != nil && object.ChunksInline.Data != nil {
		logger.GetLogger("boulder").Infof("read object %s data from inline", objkey)
		data := object.ChunksInline.Data
		if object.ChunksInline.Compress {
			data, err = utils.Decompress(data)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to decompress object %s", objkey)
				return nil, nil, fmt.Errorf("failed to decompress object %s", objkey)
			}
		}
		if int64(len(data)) != object.Size {
			logger.GetLogger("boulder").Errorf("get be damaged object data %s", objkey)
			return nil, nil, fmt.Errorf("get be damaged object data %s", objkey)
		}
		// 截取 range 部分
		if start > 0 || end < int64(len(data))-1 {
			data = data[start : end+1]
		}
		if len(data) > 0 {
			reader := bytes.NewReader(data)
			readCloser := io.NopCloser(reader) // 包装成 ReadCloser
			return object, readCloser, nil
		}
		logger.GetLogger("boulder").Errorf("failed to get the range of object %s , err %v", objkey, err)
		return nil, nil, fmt.Errorf("failed to get the range of object %s , err %v", objkey, err)
	}

	cs := chunk.GetChunkService()
	if cs == nil {
		logger.GetLogger("boulder").Errorf("failed to get the chunk service")
		return nil, nil, fmt.Errorf("failed to get the chunk service")
	}
	chunks, err := cs.BatchGet(object.Chunks)
	if err != nil || chunks == nil || len(chunks) != len(object.Chunks) {
		logger.GetLogger("boulder").Errorf("failed to get the object %d chunks", len(object.Chunks))
		return nil, nil, fmt.Errorf("failed to get the object %d chunks", len(object.Chunks))
	}
	offset := int64(0)
	blockIDs := make(map[string]*meta.Block, 0)
	for _, _chunk := range chunks {
		offset += int64(_chunk.Size)
		if offset < start {
			continue
		}
		blockIDs[_chunk.BlockID] = nil
		if offset > end {
			break
		}
	}
	bids := make([]string, 0)
	for bid, _ := range blockIDs {
		bids = append(bids, bid)
	}
	bs := block.GetBlockService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get the block service")
		return nil, nil, fmt.Errorf("failed to get the block service")
	}
	blocks, err := bs.BatchGet(bids)
	if err != nil || blocks == nil || len(blocks) != len(bids) {
		logger.GetLogger("boulder").Errorf("failed to get the block meta %d:%d", len(bids), len(blocks))
		return nil, nil, fmt.Errorf("failed to get the block meta")
	}
	for _, _block := range blocks {
		blockIDs[_block.ID] = _block
	}

	// 寻找合适的后端存储点
	storageClass := params.StorageClass
	if storageClass == "" {
		storageClass = "STANDARD"
	}

	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("boulder").Errorf("failed to get storage service")
		return nil, nil, fmt.Errorf("failed to get storage service")
	}

	scs := ss.GetStoragesByClass(storageClass)
	if len(scs) == 0 {
		logger.GetLogger("boulder").Errorf("no storage class %s", storageClass)
		return nil, nil, fmt.Errorf("no storage class %s", storageClass)
	}
	sc := scs[0]

	// 按顺序读出object 的chunk 数据
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close() // 确保无论成功失败都要关闭写端

		// 创建一个 MD5 哈希器
		hasher := md5.New()
		var writer io.Writer

		// 使用 MultiWriter，同时写入 pw 和 hasher
		writer = io.MultiWriter(pw, hasher)

		offset = 0
		num := 0
		blockDatas := make(map[string]*meta.BlockData, 0)
		for _, _chunk := range chunks {
			if offset+int64(_chunk.Size) < start {
				offset += int64(_chunk.Size)
				continue // 还没到起始位置
			}

			_blockdata := blockDatas[_chunk.BlockID]
			if _blockdata == nil {
				_bd, err := bs.ReadBlock(sc.ID, _chunk.BlockID)
				if err != nil || _bd == nil || len(_bd.Data) == 0 {
					logger.GetLogger("boulder").Errorf("failed to get the block data")
					return
				}
				_blockdata = _bd
				blockDatas[_chunk.BlockID] = _blockdata
			}

			// 从_data 中 读取 chunk 内容
			block_offset := int64(0)
			var chunkData []byte
			for _, item := range _blockdata.ChunkList {
				if item.Hash != _chunk.Hash {
					block_offset += int64(item.Size)
					continue
				}
				logger.GetLogger("boulder").Debugf("object size %d chunk %#v, block size %d:%d, block offset %d item size %d",
					object.Size, _chunk, _blockdata.TotalSize, len(_blockdata.Data), block_offset, item.Size)
				chunkData = _blockdata.Data[block_offset : block_offset+int64(item.Size)]
				break
			}
			if offset+int64(len(chunkData)) > end+1 {
				_size := end - offset + 1
				chunkData = chunkData[:_size]
			}
			if start > offset {
				_begin := start - offset
				chunkData = chunkData[_begin:]
			}

			if len(chunkData) == 0 {
				logger.GetLogger("boulder").Errorf("failed to get the chunk data from block %s", _chunk.BlockID)
				return
			}

			offset += int64(_chunk.Size)

			// 写入数据（同时写给 pw 和 hasher）
			if _, err := writer.Write(chunkData); err != nil {
				// 注意：这里不能用 pw.CloseWithError，因为 writer 是 MultiWriter
				_ = pw.CloseWithError(fmt.Errorf("failed to write chunk data: %w", err))
				return
			}
			num += len(chunkData)

			if offset > end {
				// 已读够，提前结束
				logger.GetLogger("boulder").Debugf("finish to write the object data from reader %s offset %d end %d object size %d, num %d ", _chunk.BlockID, offset, end, object.Size, num)
				break
			}
		}

		// 所有数据写完，计算最终 MD5
		finalMD5 := hasher.Sum(nil) // []byte 类型，16 字节
		finalMD5Hex := hex.EncodeToString(finalMD5)
		// 检查计算的MD5是否与对象的ETag一致
		if object.ETag != finalMD5Hex {
			logger.GetLogger("boulder").Errorf("get object %s/%s MD5 mismatch: stored=%s calculated=%s range[%d-%d]", object.Bucket, object.Key, object.ETag, finalMD5Hex, start, end)
		}
	}()

	logger.GetLogger("boulder").Debugf("put object %#v", object)
	return object, pr, nil
}

// ListObjects 实现S3兼容的对象列表功能
// 返回符合S3 ListObjects V1规范的响应结构
func (o *ObjectService) ListObjects(bucket, accessKeyID, prefix, marker, delimiter string, maxKeys int) ([]*meta.Object, error) {
	// 验证参数
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000 // S3默认值和最大值
	}

	logger.GetLogger("boulder").Debugf("ListObjects request: bucket=%s, prefix=%s, marker=%s, delimiter=%s, maxKeys=%d",
		bucket, prefix, marker, delimiter, maxKeys)

	// 验证访问密钥
	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		return nil, errors.New("failed to get iam service")
	}

	ak, err := iamService.GetAccessKey(accessKeyID)
	if err != nil || ak == nil {
		logger.GetLogger("boulder").Errorf("failed to get access key %s", accessKeyID)
		return nil, xhttp.ToError(xhttp.ErrAccessDenied)
	}

	// 检查存储桶是否存在
	bucketKey := "aws:bucket:" + ak.AccountID + ":" + bucket
	var bucketMeta meta.BucketMetadata
	if exists, err := o.kvstore.Get(bucketKey, &bucketMeta); !exists || err != nil {
		logger.GetLogger("boulder").Errorf("bucket %s does not exist", bucket)
		return nil, xhttp.ToError(xhttp.ErrNoSuchBucket)
	}

	// 构建对象键前缀
	prefixKey := "aws:object:" + ak.AccountID + ":" + bucket + "/"
	if prefix != "" {
		prefixKey += prefix
	}

	// 设置标记
	startKey := prefixKey
	if marker != "" {
		startKey = "aws:object:" + ak.AccountID + ":" + bucket + "/" + marker
	}

	// 使用KV存储的Scan方法获取对象键
	txn, err := o.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer txn.Rollback()

	// 获取对象键列表
	keys := make([]string, 0)
	nextKey := startKey
	for len(keys) < maxKeys+1 {
		scanKeys, next, err := txn.Scan(prefixKey, nextKey, maxKeys+1-len(keys))
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to scan objects prefix %s err: %v", prefixKey, err)
			return nil, fmt.Errorf("failed to scan objects prefix %s err: %v", prefixKey, err)
		}

		keys = append(keys, scanKeys...)
		if next == "" {
			break
		}
		nextKey = next
	}
	txn.Rollback()

	// 获取object meta 信息
	objects := make([]*meta.Object, 0)
	batchSize := 100
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batchKeys := keys[i:end]
		data, err := o.kvstore.BatchGet(batchKeys)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to batch ge objects: %v", err)
			return nil, fmt.Errorf("failed to batch get objects: %v", err)
		}
		for _, item := range data {
			var _obj meta.Object
			err := json.Unmarshal(item, &_obj)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to  arshal objects: %v", err)
				return nil, fmt.Errorf("failed to  arshal objects: %v", err)
			}
			objects = append(objects, &_obj)
		}
	}

	return objects, nil
}

func (o *ObjectService) DeleteObject(params *BaseObjectParams) error {
	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		return errors.New("failed to get iam service")
	}

	ak, err := iamService.GetAccessKey(params.AccessKeyID)
	if err != nil || ak == nil {
		logger.GetLogger("boulder").Errorf("failed to get access key %s", params.AccessKeyID)
		return xhttp.ToError(xhttp.ErrAccessDenied)
	}

	// 检查object 是否存在
	txn, err := o.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer txn.Rollback()
	objkey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
	var _object meta.Object
	exists, err := txn.Get(objkey, &_object)
	if !exists || err != nil {
		logger.GetLogger("boulder").Errorf("object %s does not exist", objkey)
		return xhttp.ToError(xhttp.ErrNoSuchKey)
	}
	// 删除obj 关联的chunk
	if exists && len(_object.Chunks) > 0 {
		gckey := task.GCChunkPrefix + utils.GenUUID()
		err = txn.Set(gckey, &_object.Chunks)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s set task chunk failed: %v", _object.Bucket, _object.Key, err)
			return fmt.Errorf("%s/%s set task chunk failed: %v", _object.Bucket, _object.Key, err)
		} else {
			logger.GetLogger("boulder").Infof("%s/%s set gc chunk %s delay to proccess", _object.Bucket, _object.Key, gckey)
		}
	}
	err = txn.Delete(objkey)
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s delete object failed: %v", _object.Bucket, _object.Key, err)
		return fmt.Errorf("%s/%s delete object failed: %v", _object.Bucket, _object.Key, err)
	}

	err = txn.Commit()
	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s commit object failed: %v", _object.Bucket, _object.Key, err)
		return fmt.Errorf("%s/%s commit object failed: %v", _object.Bucket, _object.Key, err)
	}
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		cache.Del(context.Background(), objkey)
	}
	return nil
}
