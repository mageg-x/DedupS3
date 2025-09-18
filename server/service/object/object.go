/*
 * Copyright (C) 2025-2025 raochaoxun <raochaoxun@gmail.com>.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package object

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/service/block"
	"github.com/mageg-x/boulder/service/chunk"
	"github.com/mageg-x/boulder/service/task"

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
	BucketName              string
	ObjKey                  string
	DestObjKey              string
	AccessKeyID             string
	StorageClass            string
	StorageID               string
	ContentLen              int64
	ContentType             string
	ContentMd5              string
	Range                   *xhttp.HTTPRangeSpec
	IfMatch                 string
	IfNoneMatch             string
	IfModifiedSince         string
	IfUnmodifiedSince       string
	SourceIfMatch           string
	SourceIfNoneMatch       string
	SourceIfModifiedSince   string
	SourceIfUnmodifiedSince string
	UploadID                string
	PartNumber              int64
	MaxParts                int64
	UploadIDMarker          string
	MaxUploads              int64
	Delimiter               string
	KeyMarker               string
	Encodingtype            string
	Prefix                  string
	ClientToken             string
}

type DeleteObjectsRequest struct {
	XMLName xml.Name `xml:"Delete"`
	XMLNS   string   `xml:"xmlns,attr"` // S3标准命名空间，固定值为http://s3.amazonaws.com/doc/2006-03-01/

	Objects []DeleteObject `xml:"Object"`
	Quiet   *bool          `xml:"Quiet,omitempty"`
}

type DeleteObject struct {
	Key              string     `xml:"Key"`
	VersionId        *string    `xml:"VersionId,omitempty"`
	ETag             *string    `xml:"ETag,omitempty"`
	LastModifiedTime *time.Time `xml:"LastModifiedTime,omitempty"`
	Size             *int64     `xml:"Size,omitempty"`
}

type DeletedObject struct {
	Key                   string  `xml:"Key"`
	VersionId             *string `xml:"VersionId,omitempty"`
	DeleteMarker          *bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionId *string `xml:"DeleteMarkerVersionId,omitempty"`
}
type DeletedObjectErrors struct {
	Key       *string `xml:"Key,omitempty"`
	VersionId *string `xml:"VersionId,omitempty"`
	Code      string  `xml:"Code"`
	Message   string  `xml:"Message"`
}

// DeleteObjectsResponse represents the response for S3 DeleteObjects API
// XMLName字段必须保留，因为Go的xml包默认会使用结构体名称(DeleteObjectsResponse)作为XML元素名，
// 但S3规范要求根元素名为"DeleteResult"
// XMLNS字段设置为S3标准命名空间
// 注意：根据S3规范，必须包含命名空间，因此不使用omitempty标签
type DeleteObjectsResponse struct {
	XMLName xml.Name              `xml:"DeleteResult"`
	XMLNS   string                `xml:"xmlns,attr"` // S3标准命名空间，固定值为http://s3.amazonaws.com/doc/2006-03-01/
	Deleted []DeletedObject       `xml:"Deleted"`
	Errors  []DeletedObjectErrors `xml:"Error"`
}

// ListObjectsResponse 对应 S3 ListObjects V1 响应
type ListObjectsResponse struct {
	XMLName xml.Name `xml:"ListBucketResult" json:"-"`
	// 命名空间属性 - 注意：根据S3规范，必须包含命名空间，因此不使用omitempty标签
	XMLNS string `xml:"xmlns,attr"`
	Name  string `xml:"Name"` // Bucket 名称

	// 可选字段（omitempty 控制：nil 或零值时不输出）
	Prefix       string `xml:"Prefix,omitempty"`
	Marker       string `xml:"Marker,omitempty"`
	MaxKeys      int    `xml:"MaxKeys,omitempty"`
	Delimiter    string `xml:"Delimiter,omitempty"`
	IsTruncated  bool   `xml:"IsTruncated,omitempty"`
	NextMarker   string `xml:"NextMarker,omitempty"`
	EncodingType string `xml:"EncodingType,omitempty"`

	// Contents 列表（对象条目）
	Contents       []ObjectContent `xml:"Contents,omitempty"`
	CommonPrefixes []CommonPrefix  `xml:"CommonPrefixes,omitempty"`
}

// ListObjectsV2Response 对应 S3 ListObjects V2 响应
type ListObjectsV2Response struct {
	XMLName xml.Name `xml:"ListBucketResult" json:"-"`
	// 命名空间属性 - 注意：根据S3规范，必须包含命名空间，因此不使用omitempty标签
	XMLNS string `xml:"xmlns,attr"`
	Name  string `xml:"Name"` // Bucket 名称

	// 可选字段（omitempty 控制：nil 或零值时不输出）
	Prefix                string `xml:"Prefix,omitempty"`
	Delimiter             string `xml:"Delimiter,omitempty"`
	MaxKeys               int    `xml:"MaxKeys,omitempty"`
	EncodingType          string `xml:"EncodingType,omitempty"`
	IsTruncated           bool   `xml:"IsTruncated,omitempty"`
	KeyCount              int    `xml:"KeyCount"`
	ContinuationToken     string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`
	StartAfter            string `xml:"StartAfter,omitempty"`

	// Contents 列表（对象条目）
	Contents       []ObjectContent `xml:"Contents,omitempty"`
	CommonPrefixes []CommonPrefix  `xml:"CommonPrefixes,omitempty"`
}

// ObjectContent 表示一个对象条目
type ObjectContent struct {
	Key               string         `xml:"Key"`
	LastModified      time.Time      `xml:"LastModified"`
	ETag              meta.Etag      `xml:"ETag"`
	Size              int64          `xml:"Size"`
	StorageClass      string         `xml:"StorageClass"`
	Owner             *meta.Owner    `xml:"Owner"`
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

// CopyObjectConditions 所有条件参数
type CopyObjectConditions struct {
	// 目标条件
	IfMatch         string
	IfNoneMatch     string
	IfModifiedSince string

	// 源条件
	SourceIfMatch           string
	SourceIfNoneMatch       string
	SourceIfModifiedSince   string
	SourceIfUnmodifiedSince string
}

// CopyObjectInfo 对象元信息
type CopyObjectInfo struct {
	Exists       bool
	ETag         meta.Etag // 建议带引号，如 "abc123"
	LastModified time.Time
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
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to fetch object %s: %v", objkey, err)
			return nil, err
		}
		if !exist {
			logger.GetLogger("boulder").Errorf("object %s does not exist", objkey)
			return nil, xhttp.ToError(xhttp.ErrNoSuchKey)
		}
		object = &_object
		if cache, e := xcache.GetCache(); e == nil && cache != nil {
			_ = cache.Set(context.Background(), objkey, object, time.Second*600)
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
			_ = cache.Set(context.Background(), key, bucket, time.Second*600)
		}
	}

	// 检查目标对象是否已经存在
	dskobjKey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
	var _dstobj meta.Object
	dstobiOk, _ := o.kvstore.Get(dskobjKey, &_dstobj)
	// 	If-Match 只有当目标对象存在且 ETag 匹配时才允许上传
	if params.IfMatch != "" {
		if !dstobiOk || string(_dstobj.ETag) != params.IfMatch {
			logger.GetLogger("boulder").Errorf("object %s/%s if match not match %s:%s", params.BucketName, params.ObjKey, params.IfMatch, _dstobj.ETag)
			return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
		}
	}
	// If-None-Match: 只有当目标不存在（*）或 ETag 不匹配时才允许上传
	if params.IfNoneMatch != "" {
		if params.IfNoneMatch == "*" {
			// * 表示：目标必须不存在
			if dstobiOk {
				logger.GetLogger("boulder").Errorf("object %s/%s already exists (If-None-Match: *)", params.BucketName, params.ObjKey)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		} else {
			// 指定了 ETag：当目标存在且 ETag 匹配时，拒绝写入
			if dstobiOk && string(_dstobj.ETag) == params.IfNoneMatch {
				logger.GetLogger("boulder").Errorf("object %s/%s ETag matches If-None-Match: %s", params.BucketName, params.ObjKey, params.IfNoneMatch)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		}
	}

	//If-Modified-Since	只有当目标对象存在且已修改（Last-Modified > 指定时间）才允许上传
	if t, err := http.ParseTime(params.IfModifiedSince); err == nil {
		if !dstobiOk || !_dstobj.LastModified.After(t) {
			logger.GetLogger("boulder").Errorf("object %s/%s modified since %s", params.BucketName, params.ObjKey, t)
			return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
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
	objectInfo.ETag = meta.Etag(params.ContentMd5)
	objectInfo.Size = params.ContentLen
	objectInfo.Owner = meta.Owner{
		ID:          ak.AccountID,
		DisplayName: ak.Username,
	}
	objectInfo.LastModified = time.Now().UTC()
	logger.GetLogger("boulder").Debugf("put object %#v", objectInfo)

	// 进行chunk切分
	chunker := chunk.GetChunkService()
	if chunker == nil {
		logger.GetLogger("boulder").Errorf("failed to get chunk service")
		return nil, fmt.Errorf("failed to get chunk service")
	}

	defer func() {
		objKey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
		if cache, err := xcache.GetCache(); err == nil && cache != nil {
			_ = cache.Del(context.Background(), objKey)
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
			objectInfo.ETag = meta.Etag(hex.EncodeToString(hash[:]))
			objectInfo.Size = int64(len(bodyBytes))
			// 直接写meta
			objPrefix := "aws:object:"
			err = chunker.WriteMeta(context.Background(), ak.AccountID, nil, nil, objectInfo, objPrefix)
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

	err = chunker.DoChunk(r, meta.ObjectToBaseObject(objectInfo), o.WriteObjectMeta)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to chunk object: %v", err)
	}

	return objectInfo, err
}

func (o *ObjectService) WriteObjectMeta(cs *chunk.ChunkService, chunks []*meta.Chunk, blocks map[string]*meta.Block, object *meta.BaseObject) error {
	var txErr error
	maxRetry := 3
	obj := meta.BaseObjectToObject(object)

	// 重试三次， 在文件相同时候，并发上传，会造成 事务冲突
	for i := 0; i < maxRetry; i++ {
		// 备份 allchunk, blocks, obj
		bakAllChunks := make([]*meta.Chunk, 0, len(chunks))
		for _, ck := range chunks {
			newChunk := ck.Clone()
			bakAllChunks = append(bakAllChunks, newChunk)
		}

		bakBlocks := make(map[string]*meta.Block, len(blocks))
		for k, v := range blocks {
			newBlock := v.Clone(false)
			bakBlocks[k] = newBlock
		}

		bakObj := obj.Clone()
		objPrefix := "aws:object:"
		txErr = cs.WriteMeta(context.Background(), obj.Owner.ID, bakAllChunks, bakBlocks, bakObj, objPrefix)
		if txErr == nil {
			break
		} else if errors.Is(txErr, kv.ErrTxnCommit) && i < maxRetry-1 {
			// 事务提交冲突
			logger.GetLogger("boulder").Warnf("transmission write object %s/%s commit failed: %v, and  retry %d times", obj.Bucket, obj.Key, txErr, i+1)
			baseDelay := 500 * time.Millisecond
			jitter := time.Duration(rand.Int63n(100)) * time.Millisecond
			sleep := baseDelay<<uint(i) + jitter
			time.Sleep(sleep)
		} else {
			logger.GetLogger("boulder").Errorf("transmission write object %s/%s  meta info failed: %v，retry times %d", obj.Bucket, obj.Key, txErr, i+1)
		}
	}
	return txErr
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
				_ = cache.Del(context.Background(), objkey)
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
			_ = cache.Set(context.Background(), objkey, object, time.Second*600)
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
	chunks, err := cs.BatchGet(object.DataLocation, object.Chunks)
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
	for bid := range blockIDs {
		bids = append(bids, bid)
	}
	bs := block.GetBlockService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get the block service")
		return nil, nil, fmt.Errorf("failed to get the block service")
	}
	blocks, err := bs.BatchGet(object.DataLocation, bids)
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
			if offset+int64(_chunk.Size) <= start {
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
				logger.GetLogger("boulder").Errorf("failed to get the chunk data from block %s start %d end %d offset %d block_offset %d chunk size %d  block header",
					_chunk.BlockID, start, end, offset, block_offset, _chunk.Size)
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
		if string(object.ETag) != finalMD5Hex {
			logger.GetLogger("boulder").Debugf("get object %s/%s MD5 mismatch: stored=%s calculated=%s range[%d-%d]", object.Bucket, object.Key, object.ETag, finalMD5Hex, start, end)
		}
	}()

	logger.GetLogger("boulder").Debugf("put object %#v", object)
	return object, pr, nil
}

// ListObjects 实现 S3 兼容的对象列表功能
func (o *ObjectService) ListObjects(bucket, accessKeyID, prefix, marker, delimiter string, maxKeys int) (objects []*meta.Object, commonPrefixes []string, isTruncated bool, nextMarker string, err error) {
	// 设置 maxKeys 上限
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	logger.GetLogger("boulder").Debugf(
		"ListObjects request: bucket=%s, prefix=%s, marker=%s, delimiter=%s, maxKeys=%d",
		bucket, prefix, marker, delimiter, maxKeys,
	)

	// 验证访问密钥
	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		return nil, nil, false, "", errors.New("failed to get iam service")
	}

	ak, err := iamService.GetAccessKey(accessKeyID)
	if err != nil || ak == nil {
		logger.GetLogger("boulder").Errorf("failed to get access key %s", accessKeyID)
		return nil, nil, false, "", xhttp.ToError(xhttp.ErrAccessDenied)
	}

	// 检查存储桶是否存在
	bucketKey := "aws:bucket:" + ak.AccountID + ":" + bucket
	var bucketMeta meta.BucketMetadata
	if exists, err := o.kvstore.Get(bucketKey, &bucketMeta); !exists || err != nil {
		logger.GetLogger("boulder").Errorf("bucket %s does not exist", bucket)
		return nil, nil, false, "", xhttp.ToError(xhttp.ErrNoSuchBucket)
	}

	// 构建 KV 存储中的前缀
	accountPrefix := "aws:object:" + ak.AccountID + ":" + bucket + "/"
	storePrefix := accountPrefix
	if prefix != "" {
		storePrefix += prefix // 如: aws:object:acc:bkt:logs/
	}

	// 设置扫描起点
	startKey := storePrefix
	if marker != "" {
		startKey = accountPrefix + marker
	}

	// 开启事务
	txn, err := o.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return nil, nil, false, "", fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	if delimiter != "" {
		// 模式 1: 有 delimiter → 启用 CommonPrefixes（分组模式）
		seenPrefixes := make(map[string]struct{})
		collected := 0
		nextKey := startKey
		var next string
		var lastProcessedKey string
		// 设置批次大小，动态调整
		batchSize := 100
		if maxKeys < 100 {
			batchSize = maxKeys
		}
		for collected < maxKeys {
			scanKeys, n, err := txn.Scan(storePrefix, nextKey, batchSize)
			if err != nil {
				logger.GetLogger("boulder").Errorf("scan failed: %v", err)
				return nil, nil, false, "", err
			} else {
				logger.GetLogger("boulder").Debugf("get scanKeys %#v", scanKeys)
			}
			if len(scanKeys) == 0 {
				break
			}
			next = n

			// 批量获取对象源数据，减少IO操作
			objMaps, err := txn.BatchGet(scanKeys)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to batch ge objects: %v", err)
				return nil, nil, false, "", fmt.Errorf("failed to batch get objects: %v", err)
			}

			for storeKey, data := range objMaps {

				// 超出 bucket 范围则停止
				if !strings.HasPrefix(storeKey, accountPrefix) {
					next = ""
					break
				}
				// 提取对象 key（去掉前缀）
				objectKey := strings.TrimPrefix(storeKey, accountPrefix)

				// 如果设置了 prefix，跳过不匹配的
				if prefix != "" && !strings.HasPrefix(objectKey, prefix) {
					next = ""
					break
				}
				lastProcessedKey = objectKey

				var _obj meta.Object
				err := json.Unmarshal(data, &_obj)
				if err != nil {
					logger.GetLogger("boulder").Errorf("failed to  arshal objects: %v", err)
					return nil, nil, false, "", fmt.Errorf("failed to  arshal objects: %v", err)
				}

				// 查找第一级 delimiter
				afterPrefix := objectKey[len(prefix):]
				delimPos := strings.Index(afterPrefix, delimiter)
				if delimPos != -1 {
					// 提取第一级目录：logs/2024/
					fullPos := len(prefix) + delimPos + len(delimiter)
					commonPrefix := objectKey[:fullPos]

					if _, exists := seenPrefixes[commonPrefix]; !exists {
						seenPrefixes[commonPrefix] = struct{}{}
						commonPrefixes = append(commonPrefixes, commonPrefix)
						collected++

						if collected >= maxKeys {
							break
						}
					}
				} else {
					// 无 delimiter 在剩余部分 → 是叶子文件
					objects = append(objects, &_obj)
					collected++
					if collected >= maxKeys {
						break
					}
				}
			}
			if collected >= maxKeys {
				break
			}
			if next == "" {
				break
			}
			nextKey = next
		}
		// 设置 isTruncated 和 nextMarker
		isTruncated = (next != "")

		if isTruncated {
			if len(objects) > 0 {
				nextMarker = objects[len(objects)-1].Key
			} else if len(commonPrefixes) > 0 {
				nextMarker = commonPrefixes[len(commonPrefixes)-1]
			} else if lastProcessedKey != "" {
				nextMarker = lastProcessedKey
			}
		}
	} else {
		// 模式 2: 无 delimiter → 平铺模式，只返回 Contents
		collected := 0
		nextKey := startKey
		var next string
		var lastProcessedKey string
		for collected < maxKeys {
			// 动态调整批次大小
			batchSize := maxKeys - collected
			if batchSize > 500 {
				batchSize = 500
			}

			scanKeys, n, err := txn.Scan(storePrefix, nextKey, batchSize)
			if err != nil {
				logger.GetLogger("boulder").Errorf("scan failed: %v", err)
				return nil, nil, false, "", err
			}
			if len(scanKeys) == 0 {
				break
			}
			next = n

			// 批量获取对象源数据，减少IO操作
			objMaps, err := txn.BatchGet(scanKeys)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to batch ge objects: %v", err)
				return nil, nil, false, "", fmt.Errorf("failed to batch get objects: %v", err)
			}
			for storeKey, data := range objMaps {
				if !strings.HasPrefix(storeKey, accountPrefix) {
					break
				}

				objectKey := strings.TrimPrefix(storeKey, accountPrefix)
				if prefix != "" && !strings.HasPrefix(objectKey, prefix) {
					break
				}

				lastProcessedKey = objectKey

				var _obj meta.Object
				err := json.Unmarshal(data, &_obj)
				if err != nil {
					logger.GetLogger("boulder").Errorf("failed to  arshal objects: %v", err)
					return nil, nil, false, "", fmt.Errorf("failed to  arshal objects: %v", err)
				}
				objects = append(objects, &_obj)
				collected++
				if collected >= maxKeys {
					break
				}
			}

			if collected >= maxKeys {
				break
			}
			if next == "" {
				break
			}
			nextKey = next
		}
		// 设置 isTruncated 和 nextMarker
		isTruncated = (next != "")

		if isTruncated && len(objects) > 0 {
			nextMarker = objects[len(objects)-1].Key
		} else if isTruncated && lastProcessedKey != "" {
			nextMarker = lastProcessedKey
		}
	}

	// 提前关闭事务
	txn.Rollback()
	txn = nil
	logger.GetLogger("boulder").Debugf("get commonPrefixs %d object %d", len(commonPrefixes), len(objects))
	return objects, commonPrefixes, isTruncated, nextMarker, nil
}

// ListObjectsV2 实现S3兼容的对象列表功能（V2版本）
func (o *ObjectService) ListObjectsV2(bucket, accessKeyID, prefix, continuationToken, startAfter, delimiter string, maxKeys int) (objects []*meta.Object, commonPrefixes []string, isTruncated bool, nextToken string, err error) {
	// 1. 解码 continuationToken → 得到 marker
	var marker string
	// 优先使用 StartAfter
	if startAfter != "" {
		marker = startAfter
	} else if continuationToken != "" {
		decoded, err := base64.StdEncoding.DecodeString(continuationToken)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to decode continuation token: %s", continuationToken)
			return nil, nil, false, "", xhttp.ToError(xhttp.ErrInvalidQueryParams)
		}
		marker = string(decoded)
	}

	// 调用V1版本的方法获取对象列表
	obs, cp, t, nm, e := o.ListObjects(bucket, accessKeyID, prefix, marker, delimiter, maxKeys)
	nextToken = base64.StdEncoding.EncodeToString([]byte(nm))
	return obs, cp, t, nextToken, e
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
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()
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
		gcChunks := task.GCChunk{
			StorageID: _object.DataLocation,
			ChunkIDs:  append([]string(nil), _object.Chunks...),
		}
		err = txn.Set(gckey, &gcChunks)
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
	txn = nil

	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		// obj
		_ = cache.Del(context.Background(), objkey)
		// chunk
		chunkKeys := make([]string, 0, len(_object.Chunks))
		for _, hash := range _object.Chunks {
			chunkKey := meta.GenChunkKey(_object.DataLocation, hash)
			chunkKeys = append(chunkKeys, chunkKey)
		}
		_ = cache.BatchDel(context.Background(), chunkKeys)
	}
	return nil
}

func (o *ObjectService) CopyObject(srcBucket, srcObject string, params *BaseObjectParams) (*meta.Object, error) {
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

	// 检查 源object 是否存在
	srcobjkey := "aws:object:" + ak.AccountID + ":" + srcBucket + "/" + srcObject
	var srcobj meta.Object
	srcobjOK, err := o.kvstore.Get(srcobjkey, &srcobj)
	if !srcobjOK || err != nil {
		logger.GetLogger("boulder").Errorf("object %s does not exist", srcobjkey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchKey)
	}

	// 检查目标桶名是否 合法
	if err := utils.CheckValidObjectName(params.ObjKey); err != nil {
		logger.GetLogger("boulder").Errorf("invalid object name: %s", params.ObjKey)
		return nil, xhttp.ToError(xhttp.ErrInvalidObjectName)
	}
	// 检查 目标桶是否存在
	dstbucketkey := "aws:bucket:" + ak.AccountID + ":" + params.BucketName
	var dstbucket meta.BucketMetadata
	exists, err := o.kvstore.Get(dstbucketkey, &dstbucket)
	if !exists || err != nil {
		logger.GetLogger("boulder").Errorf("bucket %s does not exist", dstbucketkey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchBucket)
	}
	if srcobjkey == dstbucketkey {
		logger.GetLogger("boulder").Errorf("same object %s copy", dstbucketkey)
		return nil, fmt.Errorf("same object %s copy", dstbucketkey)
	}

	// 检查目标对象是否已经存在
	dstobjkey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
	var _dstobj meta.Object
	dstobjOk, _ := o.kvstore.Get(dstobjkey, &_dstobj)
	_dst := CopyObjectInfo{Exists: dstobjOk, ETag: _dstobj.ETag, LastModified: _dstobj.LastModified}
	_src := CopyObjectInfo{Exists: srcobjOK, ETag: srcobj.ETag, LastModified: srcobj.LastModified}
	_cond := CopyObjectConditions{
		IfMatch:                 params.IfMatch,
		IfNoneMatch:             params.IfNoneMatch,
		IfModifiedSince:         params.IfModifiedSince,
		SourceIfMatch:           params.SourceIfMatch,
		SourceIfNoneMatch:       params.SourceIfNoneMatch,
		SourceIfModifiedSince:   params.SourceIfModifiedSince,
		SourceIfUnmodifiedSince: params.SourceIfUnmodifiedSince,
	}
	ok, code := o.CanCopyObject(_dst, _src, _cond)
	if !ok {
		logger.GetLogger("boulder").Errorf("%s/%s can not copy object %d", _dstobj.Bucket, _dstobj.Key, code)
		return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
	}

	// 检查目标 存储点是否存在
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

	// 复制 源对象 的 元数据
	dstobj := srcobj.Clone()
	dstobj.Bucket = params.BucketName
	dstobj.Key = params.ObjKey
	dstobj.StorageClass = storageClass
	dstobj.DataLocation = sc.ID

	if srcobj.DataLocation == dstobj.DataLocation {
		txn, err := o.kvstore.BeginTxn(context.Background(), nil)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
			return nil, fmt.Errorf("failed to begin transaction: %v", err)
		}
		defer func() {
			if txn != nil {
				_ = txn.Rollback()
			}
		}()
		for _, chunkID := range dstobj.Chunks {
			chunkey := meta.GenChunkKey(srcobj.DataLocation, chunkID)
			var _chunk meta.Chunk
			if exists, e := txn.Get(chunkey, &_chunk); e != nil || !exists {
				logger.GetLogger("boulder").Errorf("%s/%s get chunk failed: %v", srcobj.Bucket, srcobj.Key, err)
				return nil, fmt.Errorf("%s/%s get chunk %s failed: %v", srcobj.Bucket, srcobj.Key, chunkey, err)
			}
			// 引用计数加1
			_chunk.RefCount += 1
			if e := txn.Set(chunkey, &_chunk); e != nil {
				logger.GetLogger("boulder").Errorf("%s/%s set chunk %s failed: %v", dstobj.Bucket, dstobj.Key, _chunk.Hash, err)
				return nil, fmt.Errorf("%s/%s set chunk failed: %v", dstobj.Bucket, dstobj.Key, err)
			} else {
				logger.GetLogger("boulder").Debugf("%s/%s refresh set chunk: %s", dstobj.Bucket, dstobj.Key, _chunk.Hash)
			}
		}
		dstobjKey := "aws:object:" + ak.AccountID + ":" + dstobj.Bucket + "/" + dstobj.Key
		// 如果是覆盖，需要先删除旧的索引
		var _dstobj meta.Object
		exists, err = txn.Get(dstobjKey, &_dstobj)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s get object failed: %v", _dstobj.Bucket, _dstobj.Key, err)
			return nil, fmt.Errorf("%s/%s get object failed: %v", _dstobj.Bucket, _dstobj.Key, err)
		}
		if exists && len(_dstobj.Chunks) > 0 {
			gckey := task.GCChunkPrefix + utils.GenUUID()
			gcChunks := task.GCChunk{
				StorageID: _dstobj.DataLocation,
				ChunkIDs:  append([]string(nil), _dstobj.Chunks...),
			}
			err = txn.Set(gckey, &gcChunks)
			if err != nil {
				logger.GetLogger("boulder").Errorf("%s/%s set task chunk failed: %v", _dstobj.Bucket, _dstobj.Key, err)
				return nil, fmt.Errorf("%s/%s set task chunk failed: %v", _dstobj.Bucket, _dstobj.Key, err)
			} else {
				logger.GetLogger("boulder").Infof("%s/%s set gc chunk %s delay to proccess", _dstobj.Bucket, _dstobj.Key, gckey)
			}
		}

		err = txn.Set(dstobjKey, dstobj)
		if err != nil {
			logger.GetLogger("boulder").Errorf("set object %s/%s meta info failed: %v", dstobj.Bucket, dstobj.Key, err)
			return nil, fmt.Errorf("set object %s/%s meta info failed: %v", dstobj.Bucket, dstobj.Key, err)
		} else {
			logger.GetLogger("boulder").Debugf("set object %s/%s meta  ok", dstobj.Bucket, dstobj.Key)
		}
		err = txn.Commit()
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s commit failed: %v", dstobj.Bucket, dstobj.Key, err)
			return nil, kv.ErrTxnCommit
		}
		txn = nil

		logger.GetLogger("boulder").Infof("write object %s/%s  all meta data finish", dstobj.Bucket, dstobj.Key)
		if cache, err := xcache.GetCache(); err == nil && cache != nil {
			_ = cache.Del(context.Background(), dstobjKey)
		}
		return dstobj, nil
	} else {
		// 如果 存储点不一样，是要复制数据部分的 TODO
		logger.GetLogger("boulder").Errorf("%s/%s copy object  storage class not same %s:%s", srcobj.Bucket, srcobj.Key, srcobj.StorageClass, dstobj.StorageClass)
		return nil, fmt.Errorf("%s/%s copy object  storage class not same %s:%s", srcobj.Bucket, srcobj.Key, srcobj.StorageClass, dstobj.StorageClass)
	}
}

// CanCopyObject 判断 CopyObject 是否允许
// 返回：是否允许, HTTP状态码
func (o *ObjectService) CanCopyObject(dest CopyObjectInfo, src CopyObjectInfo, cond CopyObjectConditions) (bool, int) {
	// 解析时间的闭包
	parse := func(t string) (time.Time, bool) {
		if t == "" {
			return time.Time{}, false
		}
		tm, err := http.ParseTime(t)
		return tm, err == nil
	}

	// 所有条件检查都使用短路判断

	// 1. 源：x-amz-copy-source-if-match
	if cond.SourceIfMatch != "" {
		if !src.Exists || string(src.ETag) != cond.SourceIfMatch {
			return false, http.StatusPreconditionFailed
		}
	}

	// 2. 源：x-amz-copy-source-if-none-match
	if cond.SourceIfNoneMatch != "" {
		if cond.SourceIfNoneMatch == "*" {
			if !src.Exists {
				return false, http.StatusPreconditionFailed // 源必须存在
			}
		} else {
			if src.Exists && string(src.ETag) == cond.SourceIfNoneMatch {
				return false, http.StatusPreconditionFailed
			}
		}
	}

	// 3. 源：x-amz-copy-source-if-modified-since
	if t, ok := parse(cond.SourceIfModifiedSince); ok {
		if !src.Exists || !src.LastModified.After(t) {
			return false, http.StatusPreconditionFailed
		}
	}

	// 4. 源：x-amz-copy-source-if-unmodified-since
	if t, ok := parse(cond.SourceIfUnmodifiedSince); ok {
		if !src.Exists || src.LastModified.After(t) {
			return false, http.StatusPreconditionFailed
		}
	}

	// 5. 目标：If-Match
	if cond.IfMatch != "" {
		if !dest.Exists || string(dest.ETag) != cond.IfMatch {
			return false, http.StatusPreconditionFailed
		}
	}

	// 6. 目标：If-None-Match
	if cond.IfNoneMatch != "" {
		if cond.IfNoneMatch == "*" {
			if dest.Exists {
				return false, http.StatusPreconditionFailed // 目标必须不存在
			}
		} else {
			if dest.Exists && string(dest.ETag) == cond.IfNoneMatch {
				return false, http.StatusPreconditionFailed
			}
		}
	}

	// 7. 目标：If-Modified-Since
	if t, ok := parse(cond.IfModifiedSince); ok {
		if !dest.Exists || !dest.LastModified.After(t) {
			return false, http.StatusPreconditionFailed
		}
	}

	// 所有通过
	return true, http.StatusOK
}

// PutObjectTagging 更新对象的标签
func (o *ObjectService) PutObjectTagging(params *BaseObjectParams, tags map[string]string) (*meta.Object, error) {
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
	bucketKey := "aws:bucket:" + ak.AccountID + ":" + params.BucketName
	var bucket *meta.BucketMetadata
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		data, ok, e := cache.Get(context.Background(), bucketKey)
		if e == nil && ok {
			_bucket, yes := data.(*meta.BucketMetadata)
			if yes {
				bucket = _bucket
			}
		}
	}
	if bucket == nil {
		var _bucket meta.BucketMetadata
		exist, err := o.kvstore.Get(bucketKey, &_bucket)
		if !exist || err != nil {
			logger.GetLogger("boulder").Errorf("bucket %s does not exist", params.BucketName)
			return nil, xhttp.ToError(xhttp.ErrNoSuchBucket)
		}
		bucket = &_bucket
	}

	// 检查对象是否存在
	objKey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
	var object meta.Object
	exist, err := o.kvstore.Get(objKey, &object)
	if !exist || err != nil {
		logger.GetLogger("boulder").Errorf("object %s does not exist", objKey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchKey)
	}

	// 开始事务
	txn, err := o.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 更新标签
	object.Tags = tags

	// 仅更新LastModified时间戳
	object.LastModified = time.Now().UTC()

	// 保存更新后的对象
	if err := txn.Set(objKey, &object); err != nil {
		logger.GetLogger("boulder").Errorf("failed to update object tags: %v", err)
		return nil, fmt.Errorf("failed to update object tags: %v", err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return nil, kv.ErrTxnCommit
	}
	txn = nil

	// 清除缓存
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		_ = cache.Del(context.Background(), objKey)
	}

	logger.GetLogger("boulder").Infof("successfully updated tags for object %s/%s", params.BucketName, params.ObjKey)
	return &object, nil
}

func (o *ObjectService) PutObjectAcl(params *BaseObjectParams, acp *meta.AccessControlPolicy) (*meta.Object, error) {
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
	bucketKey := "aws:bucket:" + ak.AccountID + ":" + params.BucketName
	var bucket *meta.BucketMetadata
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		data, ok, e := cache.Get(context.Background(), bucketKey)
		if e == nil && ok {
			_bucket, yes := data.(*meta.BucketMetadata)
			if yes {
				bucket = _bucket
			}
		}
	}
	if bucket == nil {
		var _bucket meta.BucketMetadata
		exist, err := o.kvstore.Get(bucketKey, &_bucket)
		if !exist || err != nil {
			logger.GetLogger("boulder").Errorf("bucket %s does not exist", params.BucketName)
			return nil, xhttp.ToError(xhttp.ErrNoSuchBucket)
		}
		bucket = &_bucket
	}

	// 检查对象是否存在
	objKey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
	var object meta.Object
	exist, err := o.kvstore.Get(objKey, &object)
	if !exist || err != nil {
		logger.GetLogger("boulder").Errorf("object %s does not exist", objKey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchKey)
	}
	// 检查acp是否为空或acp.Owner是否为空
	if acp == nil {
		logger.GetLogger("boulder").Errorf("access control policy is nil")
		return nil, xhttp.ToError(xhttp.ErrInvalidArgument)
	}

	// 检查acp.Owner是否为空
	if acp.Owner.ID == "" {
		logger.GetLogger("boulder").Errorf("owner information in access control policy is missing")
		return nil, xhttp.ToError(xhttp.ErrInvalidArgument)
	}

	// 检查acp的Owner是否与object的Owner一致
	if acp.Owner.ID != object.Owner.ID {
		logger.GetLogger("boulder").Errorf("ACL owner %s does not match object owner %s", acp.Owner.ID, object.Owner.ID)
		return nil, xhttp.ToError(xhttp.ErrAccessDenied)
	}
	// 开始事务
	txn, err := o.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 更新对象的ACL
	object.ACL = acp
	// 仅更新LastModified时间戳
	object.LastModified = time.Now().UTC()

	// 保存更新后的对象
	if err := txn.Set(objKey, &object); err != nil {
		logger.GetLogger("boulder").Errorf("failed to update object acl: %v", err)
		return nil, fmt.Errorf("failed to update object acl: %v", err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return nil, kv.ErrTxnCommit
	}
	txn = nil

	// 清除缓存
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		_ = cache.Del(context.Background(), objKey)
	}

	logger.GetLogger("boulder").Infof("successfully updated acl for object %s/%s", params.BucketName, params.ObjKey)
	return &object, nil
}

// RenameObject 重命名对象（内部实现）
// 支持幂等性：通过ClientToken参数确保同一请求多次调用不会产生不同结果
func (o *ObjectService) RenameObject(params *BaseObjectParams) (*meta.Object, error) {
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

	// 检查 源object 是否存在
	srcobjkey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
	var srcobj meta.Object
	srcObjOK, err := o.kvstore.Get(srcobjkey, &srcobj)
	if !srcObjOK || err != nil {
		logger.GetLogger("boulder").Errorf("object %s does not exist", srcobjkey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchKey)
	}

	//  检查目标对象是否存在并验证条件
	dstobjkey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.DestObjKey
	var dstObj meta.Object
	dstObjOK, err := o.kvstore.Get(dstobjkey, &dstObj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to check destination object: %v", err)
		return nil, fmt.Errorf("failed to check destination object: %v", err)
	}

	// 验证目标对象条件
	if params.SourceIfMatch != "" {
		if !srcObjOK || string(srcobj.ETag) != params.SourceIfMatch {
			logger.GetLogger("boulder").Errorf("source object %s/%s SourceIfMatch not match", params.BucketName, params.ObjKey)
			return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
		}
	}

	if params.SourceIfNoneMatch != "" {
		if params.SourceIfNoneMatch == "*" {
			// * 表示：源必须不存在
			if srcObjOK {
				logger.GetLogger("boulder").Errorf("source object %s/%s already exists (SourceIfNoneMatch: *)", params.BucketName, params.ObjKey)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		} else {
			// 指定了 ETag：当源存在且 ETag 匹配时，拒绝操作
			if srcObjOK && string(srcobj.ETag) == params.SourceIfNoneMatch {
				logger.GetLogger("boulder").Errorf("source object %s/%s ETag matches SourceIfNoneMatch: %s", params.BucketName, params.ObjKey, params.SourceIfNoneMatch)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		}
	}

	if params.SourceIfModifiedSince != "" {
		if t, err := http.ParseTime(params.SourceIfModifiedSince); err == nil {
			if !srcObjOK || !srcobj.LastModified.After(t) {
				logger.GetLogger("boulder").Errorf("source object %s/%s not modified since %s", params.BucketName, params.ObjKey, t)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		}
	}

	if params.SourceIfUnmodifiedSince != "" {
		if t, err := http.ParseTime(params.SourceIfUnmodifiedSince); err == nil {
			if !srcObjOK || srcobj.LastModified.After(t) {
				logger.GetLogger("boulder").Errorf("source object %s/%s modified since %s", params.BucketName, params.ObjKey, t)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		}
	}

	// 目标对象条件验证
	if params.IfMatch != "" {
		if !dstObjOK || string(dstObj.ETag) != params.IfMatch {
			logger.GetLogger("boulder").Errorf("target object %s/%s IfMatch not match", params.BucketName, params.DestObjKey)
			return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
		}
	}

	if params.IfNoneMatch != "" {
		if params.IfNoneMatch == "*" {
			// * 表示：目标必须不存在
			if dstObjOK {
				logger.GetLogger("boulder").Errorf("target object %s/%s already exists (IfNoneMatch: *)", params.BucketName, params.DestObjKey)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		} else {
			// 指定了 ETag：当目标存在且 ETag 匹配时，拒绝操作
			if dstObjOK && string(dstObj.ETag) == params.IfNoneMatch {
				logger.GetLogger("boulder").Errorf("target object %s/%s ETag matches IfNoneMatch: %s", params.BucketName, params.DestObjKey, params.IfNoneMatch)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		}
	}

	if params.IfModifiedSince != "" {
		if t, err := http.ParseTime(params.IfModifiedSince); err == nil {
			if !dstObjOK || !dstObj.LastModified.After(t) {
				logger.GetLogger("boulder").Errorf("target object %s/%s not modified since %s", params.BucketName, params.DestObjKey, t)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		}
	}

	if params.IfUnmodifiedSince != "" {
		if t, err := http.ParseTime(params.IfUnmodifiedSince); err == nil {
			if !dstObjOK || dstObj.LastModified.After(t) {
				logger.GetLogger("boulder").Errorf("target object %s/%s modified since %s", params.BucketName, params.DestObjKey, t)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		}
	}

	// 开始事务处理
	txn, err := o.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 如果目标 object name已经存在 ，需要先删除旧的索引
	if dstObjOK && len(dstObj.Chunks) > 0 {
		gckey := task.GCChunkPrefix + utils.GenUUID()
		gcChunks := task.GCChunk{
			StorageID: dstObj.DataLocation,
			ChunkIDs:  append([]string(nil), dstObj.Chunks...),
		}
		err = txn.Set(gckey, &gcChunks)
		if err != nil {
			logger.GetLogger("boulder").Errorf("%s/%s set task chunk failed: %v", dstObj.Bucket, dstObj.Key, err)
			return nil, fmt.Errorf("%s/%s set task chunk failed: %v", dstObj.Bucket, dstObj.Key, err)
		} else {
			logger.GetLogger("boulder").Infof("%s/%s set gc chunk %s delay to proccess", dstObj.Bucket, dstObj.Key, gckey)
		}
	}

	// 删除 源object 的 key
	err = txn.Delete(srcobjkey)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete object %s : %v", srcobjkey, err)
		return nil, fmt.Errorf("failed to delete object: %v", err)
	}

	//重新设置 新key
	srcobj.Key = params.DestObjKey
	srcobj.LastModified = time.Now().UTC()
	err = txn.Set(dstobjkey, &srcobj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to update object %s : %v", dstobjkey, err)
		return nil, fmt.Errorf("failed to update object: %v", err)
	}

	//  提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return nil, kv.ErrTxnCommit
	}
	txn = nil

	// 清除缓存
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		_ = cache.Del(context.Background(), srcobjkey)
		_ = cache.Del(context.Background(), dstobjkey)
	}

	logger.GetLogger("boulder").Infof("successfully renamed object %s to %s", params.ObjKey, params.DestObjKey)
	return &srcobj, nil
}
