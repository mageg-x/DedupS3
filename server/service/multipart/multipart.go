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
package multipart

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/mageg-x/boulder/service/gc"

	"github.com/mageg-x/boulder/service/chunk"
	"github.com/mageg-x/boulder/service/storage"

	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
	"github.com/mageg-x/boulder/service/iam"
	"github.com/mageg-x/boulder/service/object"
)

const (
	UID_PREFIX = "u0118"
)

var (
	instance *MultiPartService
	mu       = sync.Mutex{}
)

type MultiPartService struct {
	kvstore kv.KVStore
}

// InitiateMultipartUploadResult 定义 S3 Initiate Multipart Upload 的 XML 响应结构
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"` // 命名空间，固定值为http://s3.amazonaws.com/doc/2006-03-01/
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name  `xml:"CompleteMultipartUploadResult"`
	XMLNS    string    `xml:"xmlns,attr"` // 命名空间，固定值为http://s3.amazonaws.com/doc/2006-03-01/
	Location string    `xml:"Location"`
	Bucket   string    `xml:"Bucket"`
	Key      string    `xml:"Key"`
	ETag     meta.Etag `xml:"ETag"`

	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
}

type ListPartsResult struct {
	XMLName xml.Name `xml:"ListPartsResult"`
	XMLNS   string   `xml:"xmlns,attr"` // 根据S3规范，必须包含命名空间

	Bucket               string           `xml:"Bucket"`
	Key                  string           `xml:"Key"`
	UploadId             string           `xml:"UploadId"`
	PartNumberMarker     int              `xml:"PartNumberMarker"`
	NextPartNumberMarker int              `xml:"NextPartNumberMarker"`
	MaxParts             int              `xml:"MaxParts"`
	IsTruncated          bool             `xml:"IsTruncated"`
	Part                 []*meta.PartInfo `xml:"Part"`
	Initiator            meta.Initiator   `xml:"Initiator"`
	Owner                meta.Owner       `xml:"Owner"`
	StorageClass         string           `xml:"StorageClass"`

	// Checksum 相关字段（可选，根据需求启用）
	ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
}

// ListMultipartUploadsResult S3 ListMultipartUploads 响应
type ListMultipartUploadsResult struct {
	XMLName xml.Name `xml:"ListMultipartUploadsResult"`
	XMLNS   string   `xml:"xmlns,attr"` // 根据S3规范，必须包含命名空间
	// 必选字段（S3 总是返回，即使为空字符串）
	Bucket             string    `xml:"Bucket"`
	MaxUploads         int       `xml:"MaxUploads"`
	IsTruncated        bool      `xml:"IsTruncated"`
	KeyMarker          string    `xml:"KeyMarker,omitempty"`
	UploadIdMarker     string    `xml:"UploadIdMarker,omitempty"`
	NextKeyMarker      string    `xml:"NextKeyMarker,omitempty"`
	Prefix             string    `xml:"Prefix,omitempty"`
	Delimiter          string    `xml:"Delimiter,omitempty"`
	NextUploadIdMarker string    `xml:"NextUploadIdMarker,omitempty"`
	EncodingType       string    `xml:"EncodingType,omitempty"`
	Upload             []*Upload `xml:"Upload,omitempty"`
	CommonPrefixes     *[]string `xml:"CommonPrefixes>Prefix,omitempty"`
}

// MultipartUpload 分片上传任务信息
type Upload struct {
	Key               string         `xml:"Key,omitempty"`
	UploadId          string         `xml:"UploadId,omitempty"`
	StorageClass      string         `xml:"StorageClass,omitempty"`
	Initiated         string         `xml:"Initiated,omitempty"`
	Initiator         meta.Initiator `xml:"Initiator,omitempty"`
	Owner             meta.Owner     `xml:"Owner,omitempty"`
	ChecksumAlgorithm string         `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType      string         `xml:"ChecksumType,omitempty"`
}

type CopyPartResult struct {
	XMLName      xml.Name  `xml:"CopyPartResult"`
	XMLNS        string    `xml:"xmlns,attr"` // 命名空间，固定值为http://s3.amazonaws.com/doc/2006-03-01/
	ETag         meta.Etag `xml:"ETag"`
	LastModified time.Time `xml:"LastModified"`
	// Checksum 相关字段（可选，根据需求启用）
	ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
}

func GetMultiPartService() *MultiPartService {
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
	instance = &MultiPartService{
		kvstore: store,
	}
	return instance
}

// AbortMultipartUpload 中止分段上传并删除所有相关元数据
func (m *MultiPartService) AbortMultipartUpload(params *object.BaseObjectParams) error {
	// 验证访问密钥
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
	// 检查分段上传任务是否存在
	uploadKey := "aws:upload:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey + "/" + params.UploadID
	var upload meta.MultipartUpload
	exists, err := m.kvstore.Get(uploadKey, &upload)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get multipart upload: %v", err)
		return fmt.Errorf("failed to get multipart upload: %w", err)
	}
	if !exists {
		// 根据S3规范，中止不存在的上传任务应该返回204 No Content
		logger.GetLogger("boulder").Debugf("multipart upload not found, still returning success: %s", uploadKey)
		return nil
	}

	// 开始事务
	txn, err := m.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 上传过程中产生的 chunk 元素据 都要清理
	gcData := gc.GCChunk{
		GCData: gc.GCData{
			CreateAt: time.Now().UTC(),
			Items:    make([]gc.GCItem, 0),
		},
	}

	// 扫描所有 Part 元数据
	prefix := uploadKey + "/"
	startKey := ""
	for {
		keys, nextKey, err := txn.Scan(prefix, startKey, 100)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to scan multipart upload parts: %v", err)
			return fmt.Errorf("failed to scan multipart upload parts: %w", err)
		}
		partBytes, err := txn.BatchGet(keys)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get multipart upload part meta: %v", err)
			return fmt.Errorf("failed to get multipart upload part meta: %w", err)
		}
		if len(partBytes) != len(keys) {
			logger.GetLogger("boulder").Errorf("unexpected number of parts: expected %d, got %d", len(keys), len(partBytes))
			return errors.New("inconsistent part count")
		}

		for _, data := range partBytes {
			var part meta.PartObject
			if err := json.Unmarshal(data, &part); err != nil {
				logger.GetLogger("boulder").Errorf("failed to unmarshal part meta: %v", err)
				return fmt.Errorf("failed to unmarshal part meta: %w", err)
			}
			for _, id := range part.Chunks {
				gcData.Items = append(gcData.Items, gc.GCItem{StorageID: upload.DataLocation, ID: id})
			}
		}

		if len(keys) < 100 || nextKey == "" {
			break
		}
		startKey = nextKey
	}
	// 删除上传任务自己的 uploadid下的 元数据
	if err := txn.DeletePrefix(uploadKey, 0); err != nil { // 传0表示无限制删除
		logger.GetLogger("boulder").Errorf("failed to delete upload info: %v", err)
		return fmt.Errorf("failed to delete upload info: %w", err)
	}

	if len(gcData.Items) > 0 {
		gckey := gc.GCChunkPrefix + utils.GenUUID()
		err = txn.Set(gckey, &gcData)
		if err != nil {
			logger.GetLogger("boulder").Errorf("aborted multipart upload %s/%s/%s set gc chunk failed: %v", params.BucketName, params.ObjKey, params.UploadID, err)
			return fmt.Errorf("aborted multipart upload %s/%s/%s set gc chunk failed: %w", params.BucketName, params.ObjKey, params.UploadID, err)
		} else {
			logger.GetLogger("boulder").Infof("aborted multipart upload %s/%s/%s set gc chunk %s delay to proccess", params.BucketName, params.ObjKey, params.UploadID, gckey)
		}
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	txn = nil

	logger.GetLogger("boulder").Infof("aborted multipart upload: bucket=%s, key=%s, uploadID=%s",
		params.BucketName, params.ObjKey, params.UploadID)

	return nil
}

func (m *MultiPartService) WritePartMeta(cs *chunk.ChunkService, chunks []*meta.Chunk, blocks map[string]*meta.Block, object *meta.BaseObject) error {
	part := meta.BaseObjectToPart(object)
	err := utils.RetryCall(3, func() error {
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

		bakPart := part.Clone()
		objPrefix := "aws:upload:"

		return cs.WriteMeta(context.Background(), part.Owner.ID, bakAllChunks, bakBlocks, bakPart, objPrefix)
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("transmission write object %s/%s  meta info failed: %v ", part.Bucket, part.Key, err)
		return fmt.Errorf("transmission write object %s/%s  meta info failed: %w ", part.Bucket, part.Key, err)

	}
	return nil
}

func (m *MultiPartService) CreateMultipartUpload(headers http.Header, params *object.BaseObjectParams) (*meta.MultipartUpload, error) {
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

	ifMatch := headers.Get(xhttp.IfMatch)
	ifMatch = strings.Trim(ifMatch, "\"")
	ifNoneMatch := headers.Get(xhttp.IfNoneMatch)
	ifNoneMatch = strings.Trim(ifNoneMatch, "\"")
	ifModifiedSince := headers.Get(xhttp.IfModifiedSince)
	// content-type
	ct := headers.Get(xhttp.ContentType)
	if ct == "" {
		ct = "application/octet-stream"
	}

	// storage class
	storageClass := headers.Get(xhttp.AmzStorageClass)
	storageClass = strings.TrimSpace(storageClass)
	if storageClass != "" {
		if err := utils.CheckValidStorageClass(storageClass); err != nil {
			logger.GetLogger("boulder").Errorf("Invalid storage class: %s", storageClass)
			return nil, xhttp.ToError(xhttp.ErrInvalidStorageClass)
		}
	}

	if storageClass == "" {
		storageClass = meta.STANDARD_CLASS_STORAGE
	}

	bs := storage.GetStorageService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("failed to get storage service")
		return nil, errors.New("failed to get storage service")
	}

	scs := bs.GetStoragesByClass(storageClass)
	if len(scs) == 0 {
		logger.GetLogger("boulder").Errorf("no storage class %s", storageClass)
		return nil, fmt.Errorf("no storage class %s", storageClass)
	}
	sc := scs[0]

	// Disposition
	disposition := headers.Get(xhttp.ContentDisposition)
	// Content-Encoding
	encoding := headers.Get(xhttp.ContentEncoding)
	// Cache-Control
	cacheControl := headers.Get(xhttp.CacheControl)
	// Content-Language
	language := headers.Get(xhttp.ContentLanguage)
	// user meta
	userMeta, _ := utils.ExtractMetadata(headers)
	// X-Amz-Tagging
	tags, _ := utils.ExtractTags(headers)

	// 检查bucket是否存在
	key := "aws:bucket:" + ak.AccountID + ":" + params.BucketName
	var _bucket meta.BucketMetadata
	bucketOK, err := m.kvstore.Get(key, &_bucket)
	if !bucketOK || err != nil {
		logger.GetLogger("boulder").Errorf("bucket %s does not exist", params.BucketName)
		return nil, xhttp.ToError(xhttp.ErrNoSuchBucket)
	}

	// 检查object 是否已经存在
	key = "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
	var _object meta.Object
	objOK, _ := m.kvstore.Get(key, &_object)

	// ==== 条件校验（Precondition Check）====
	if ifMatch != "" {
		// If-Match: 必须匹配当前对象 ETag
		if !objOK || string(_object.ETag) != strings.Trim(ifMatch, `"`) {
			logger.GetLogger("boulder").Errorf("multi part upload %s does not match ifMatch", params.BucketName)
			return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
		}
	}

	if ifNoneMatch != "" {
		if ifNoneMatch == "*" {
			// If-None-Match: * → 对象不能存在
			if objOK {
				logger.GetLogger("boulder").Errorf("multi part upload %s does not match ifNoneMatch", params.BucketName)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		} else {
			// If-None-Match: "etag" → ETag 不能匹配
			if objOK && string(_object.ETag) == strings.Trim(ifNoneMatch, `"`) {
				logger.GetLogger("boulder").Errorf("multi part upload %s does not match ifNoneMatch", params.BucketName)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		}
	}

	if ifModifiedSince != "" {
		t, err := http.ParseTime(ifModifiedSince)
		if err == nil && objOK {
			// 如果 Last-Modified <= If-Modified-Since，则返回 412
			if !_object.LastModified.After(t) {
				logger.GetLogger("boulder").Errorf("multi part upload %s does not match ifModifiedSince", params.BucketName)
				return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
			}
		}
	}

	// 生成上传ID
	uploadID := uuid.New().String()
	segs := strings.Split(uploadID, "-")
	segs[0] = UID_PREFIX
	uploadID = strings.Join(segs, "-")
	upload := &meta.MultipartUpload{
		UploadID:           uploadID,
		Bucket:             params.BucketName,
		Key:                params.ObjKey,
		Owner:              meta.Owner{ID: ak.AccountID, DisplayName: ak.Username}, // 假设 ak.UserID 存在
		Initiator:          meta.Initiator{ID: ak.AccountID, DisplayName: ak.Username},
		StorageClass:       storageClass,
		DataLocation:       sc.ID,
		ContentType:        ct,
		ContentEncoding:    encoding,
		ContentLanguage:    language,
		CacheControl:       cacheControl,
		ContentDisposition: disposition,
		UserMetadata:       userMeta,
		Tags:               tags,
		Created:            time.Now().UTC(),
	}
	key = "aws:upload:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey + "/" + uploadID

	txn, err := m.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction for multi upload %s", key)
		return nil, fmt.Errorf("failed to begin transaction for multi upload %s: %w", key, err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()
	err = txn.Set(key, &upload)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to set multi upload meta %s uploadid %s ", key, uploadID)
		return nil, fmt.Errorf("failed to set multi upload meta %s uploadid %s: %w", key, uploadID, err)
	}
	err = txn.Commit()
	if err != nil {
		logger.GetLogger("boulder").Errorf("create multi upload %s uploadid %s  failed to commit transaction", key, uploadID)
		return nil, fmt.Errorf("create multi upload %s uploadid %s failed to commit transaction: %w", key, uploadID, err)
	} else {
		logger.GetLogger("boulder").Infof("create multi upload %s uploadid %s success", key, uploadID)
	}
	txn = nil
	return upload, nil
}

func (m *MultiPartService) UploadPart(r io.Reader, params *object.BaseObjectParams) (*meta.PartObject, error) {
	// 验证参数
	if params == nil || params.UploadID == "" || r == nil {
		logger.GetLogger("boulder").Errorf("invalid parameters for UploadPart")
		return nil, xhttp.ToError(xhttp.ErrInvalidQueryParams)
	}

	if params.PartNumber <= 0 {
		logger.GetLogger("boulder").Errorf("invalid PartNumber for UploadPart")
		return nil, xhttp.ToError(xhttp.ErrInvalidPart)
	}

	if params.PartNumber > meta.MAX_PART_ID {
		logger.GetLogger("boulder").Errorf("PartNumber %d is too large", params.PartNumber)
		return nil, xhttp.ToError(xhttp.ErrInvalidMaxParts)
	}

	if err := utils.CheckValidBucketName(params.BucketName); err != nil {
		logger.GetLogger("boulder").Errorf("not such bucket %s", params.BucketName)
		return nil, xhttp.ToError(xhttp.ErrNoSuchBucket)
	}

	if err := utils.CheckValidObjectName(params.ObjKey); err != nil {
		logger.GetLogger("boulder").Errorf("no such object %s", params.ObjKey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchKey)
	}

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

	uploadKey := "aws:upload:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey + "/" + params.UploadID

	var upload meta.MultipartUpload
	exists, err := m.kvstore.Get(uploadKey, &upload)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get upload meta %s uploadid %s", uploadKey, params.UploadID)
		return nil, xhttp.ToError(xhttp.ErrInternalError)
	}
	if !exists {
		logger.GetLogger("boulder").Errorf("no such upload meta %s", uploadKey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchUpload)
	}

	// 创建分段对象
	part := &meta.PartObject{
		BaseObject: meta.BaseObject{
			Bucket:       params.BucketName,
			Key:          fmt.Sprintf("%s/%s/%d", params.ObjKey, params.UploadID, params.PartNumber),
			ETag:         meta.Etag(params.ContentMd5),
			Size:         params.ContentLen,
			LastModified: time.Now().UTC(),
			CreatedAt:    time.Now().UTC(),
			Chunks:       make([]string, 0),
			DataLocation: upload.DataLocation,
			ObjType:      meta.PART_OBJECT,
		},
		UploadID:     params.UploadID,
		PartNumber:   int(params.PartNumber),
		Owner:        upload.Owner,
		Initiator:    upload.Initiator,
		StorageClass: upload.StorageClass,
	}

	// 进行chunk切分
	chunker := chunk.GetChunkService()
	if chunker == nil {
		logger.GetLogger("boulder").Errorf("failed to get chunk service")
		return nil, errors.New("failed to get chunk service")
	}

	err = chunker.DoChunk(r, meta.PartToBaseObject(part), m.WritePartMeta)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to chunk object: %v", err)
		return nil, fmt.Errorf("failed to chunk object: %w", err)
	}
	return part, nil
}
func (m *MultiPartService) UploadPartCopy(srcBucket, srcObject string, params *object.BaseObjectParams) (*meta.PartObject, error) {
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

	// 1. 检查源对象是否存在
	srcObjKey := "aws:object:" + ak.AccountID + ":" + srcBucket + "/" + srcObject
	var srcObj meta.Object
	srcObjExists, err := m.kvstore.Get(srcObjKey, &srcObj)
	if !srcObjExists || err != nil {
		logger.GetLogger("boulder").Errorf("source object %s does not exist", srcObjKey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchKey)
	}

	// 3. 检查目标 upload 是否存在
	uploadKey := "aws:upload:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey + "/" + params.UploadIDMarker
	var upload meta.MultipartUpload
	uploadExists, err := m.kvstore.Get(uploadKey, &upload)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get upload meta %s: %v", uploadKey, err)
		return nil, xhttp.ToError(xhttp.ErrInternalError)
	}
	if !uploadExists {
		logger.GetLogger("boulder").Errorf("no such upload %s", uploadKey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchUpload)
	}

	// 4. 构建 part 对象
	part := &meta.PartObject{
		BaseObject: meta.BaseObject{
			Bucket:       params.BucketName,
			Key:          params.ObjKey,
			LastModified: time.Now().UTC(),
			CreatedAt:    time.Now().UTC(),
			Size:         srcObj.Size,
			Chunks:       append([]string(nil), srcObj.Chunks...),
			ETag:         srcObj.ETag,
			DataLocation: upload.DataLocation,
			ObjType:      meta.PART_OBJECT,
		},
		UploadID:     params.UploadIDMarker,
		PartNumber:   int(params.PartNumber),
		Owner:        upload.Owner,
		Initiator:    upload.Initiator,
		StorageClass: upload.StorageClass,
	}

	// 只支持同源复制
	if srcObj.DataLocation == upload.DataLocation {
		txn, err := m.kvstore.BeginTxn(context.Background(), nil)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer func() {
			if txn != nil {
				_ = txn.Rollback()
			}
		}()
		// 5. 遍历源对象的 chunks，增加ref 引用计数
		for _, chunkID := range srcObj.Chunks {
			chunkey := meta.GenChunkKey(srcObj.DataLocation, chunkID)
			var _chunk meta.Chunk
			if exists, e := txn.Get(chunkey, &_chunk); e != nil || !exists {
				logger.GetLogger("boulder").Errorf("%s/%s get chunk failed: %v", srcObj.Bucket, srcObj.Key, err)
				return nil, fmt.Errorf("%s/%s get chunk %s failed: %w", srcObj.Bucket, srcObj.Key, chunkey, err)
			}
			// 引用计数加1
			_chunk.RefCount += 1
			if e := txn.Set(chunkey, &_chunk); e != nil {
				logger.GetLogger("boulder").Errorf("%s/%s set chunk %s failed: %v", upload.Bucket, upload.Key, _chunk.Hash, err)
				return nil, fmt.Errorf("%s/%s set chunk failed: %w", upload.Bucket, upload.Key, err)
			} else {
				logger.GetLogger("boulder").Debugf("%s/%s refresh set chunk: %s", upload.Bucket, upload.Key, _chunk.Hash)
			}
		}
		// 6. 保存 part 元数据
		partKey := fmt.Sprintf("aws:upload:%s:%s/%s/%s/%d",
			ak.AccountID, upload.Bucket, upload.Key, upload.UploadID, params.PartNumber)
		if err := txn.Set(partKey, part); err != nil {
			return nil, fmt.Errorf("failed to save part meta: %w", err)
		}
		if err := txn.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit part: %w", err)
		}
		txn = nil
	} else {
		// 不支持 不同类型存储点之间复制
		logger.GetLogger("boulder").Errorf("%s/%s copy object storage class not same %s:%s", srcObj.Bucket, srcObj.Key, srcObj.StorageClass, upload.StorageClass)
		return nil, fmt.Errorf("%s/%s copy object storage class not same %s:%s", srcObj.Bucket, srcObj.Key, srcObj.StorageClass, upload.StorageClass)
	}

	return part, nil
}

func (m *MultiPartService) CompleteMultipartUpload(cliParts []meta.PartETag, params *object.BaseObjectParams) (*meta.Object, error) {
	// 参数校验
	if params.AccessKeyID == "" || params.BucketName == "" || params.ObjKey == "" || params.UploadID == "" || len(cliParts) == 0 {
		logger.GetLogger("boulder").Errorf("invalid parameters for CompleteMultipartUpload")
		return nil, xhttp.ToError(xhttp.ErrInvalidQueryParams)
	}

	// 获取 IAM 服务
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

	// 检查 Upload 是否存在
	uploadKey := "aws:upload:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey + "/" + params.UploadID
	var upload meta.MultipartUpload
	exists, err := m.kvstore.Get(uploadKey, &upload)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get multipart upload: %v", err)
		return nil, fmt.Errorf("failed to get multipart upload: %w", err)
	}
	if !exists {
		logger.GetLogger("boulder").Errorf("multipart upload not found: %s", uploadKey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchUpload)
	}

	// === 条件检查开始 ===
	// 获取当前对象是否存在
	oldObjKey := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey
	var oldObj meta.Object
	existsOldObj, err := m.kvstore.Get(oldObjKey, &oldObj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get existing object %s: %v", oldObjKey, err)
		return nil, fmt.Errorf("failed to check existing object %s : %w", oldObjKey, err)
	}

	// If-None-Match: *
	if params.IfNoneMatch == "*" {
		if existsOldObj {
			return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
		}
	}

	// If-Match: "etag"
	if params.IfMatch != "" {
		if !existsOldObj {
			return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
		}

		if params.IfMatch != string(oldObj.ETag) {
			return nil, xhttp.ToError(xhttp.ErrPreconditionFailed)
		}
	}
	// === 条件检查结束 ===

	// 开启事务
	txnReadOnly, err := m.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// 扫描所有 Part 元数据
	prefix := uploadKey + "/"
	var allParts []*meta.PartObject
	startKey := ""
	for {
		keys, nextKey, err := txnReadOnly.Scan(prefix, startKey, 100)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to scan multipart upload parts: %v", err)
			return nil, fmt.Errorf("failed to scan multipart upload parts: %w", err)
		}
		partBytes, err := txnReadOnly.BatchGet(keys)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get multipart upload part meta: %v", err)
			return nil, fmt.Errorf("failed to get multipart upload part meta: %w", err)
		}
		if len(partBytes) != len(keys) {
			logger.GetLogger("boulder").Errorf("unexpected number of parts: expected %d, got %d", len(keys), len(partBytes))
			return nil, errors.New("inconsistent part count")
		}

		for _, data := range partBytes {
			var part meta.PartObject
			if err := json.Unmarshal(data, &part); err != nil {
				logger.GetLogger("boulder").Errorf("failed to unmarshal part meta: %v", err)
				return nil, fmt.Errorf("failed to unmarshal part meta: %w", err)
			}
			allParts = append(allParts, &part)
		}

		if len(keys) < 100 || nextKey == "" {
			break
		}
		startKey = nextKey
	}

	if len(allParts) == 0 {
		logger.GetLogger("boulder").Errorf("no parts found for upload %s", params.UploadID)
		return nil, xhttp.ToError(xhttp.ErrInvalidPart)
	}

	// 按 PartNumber 排序
	sort.Slice(allParts, func(i, j int) bool {
		return allParts[i].PartNumber < allParts[j].PartNumber
	})

	// 检查是否和客户端一致
	sort.Slice(cliParts, func(i, j int) bool {
		return cliParts[i].PartNumber < cliParts[j].PartNumber
	})
	if len(cliParts) != len(allParts) {
		logger.GetLogger("boulder").Errorf("client part count mismatch: expected %d, got %d", len(allParts), len(cliParts))
		return nil, xhttp.ToError(xhttp.ErrInvalidPart)
	}
	for i, p := range allParts {
		if p.ETag != cliParts[i].ETag {
			logger.GetLogger("boulder").Errorf("part etag not match expected %s, got %s", p.ETag, cliParts[i].ETag)
			return nil, xhttp.ToError(xhttp.ErrInvalidPart)
		}
		//除了最后一个其他都要大于 5M
		if i < len(allParts)-1 && p.Size < meta.MIN_PART_SIZE {
			logger.GetLogger("boulder").Errorf("the none last part size %d is smaller then %d", p.Size, meta.MIN_PART_SIZE)
			return nil, xhttp.ToError(xhttp.ErrInvalidPart)
		}
	}

	// 检查连续性
	for idx, p := range allParts {
		if (idx + 1) != p.PartNumber {
			logger.GetLogger("boulder").Errorf("invalid sequence part number: %d", p.PartNumber)
			return nil, xhttp.ToError(xhttp.ErrInvalidPartOrder)
		}
	}

	// 验证 ETag，并计算最终 ETag
	hash := md5.New()
	totalSize := int64(0)
	Chunks := make([]string, 0)

	for _, p := range allParts {
		partETag := string(p.ETag)
		// 解码原始 MD5 并写入 hash
		binaryMD5, err := hex.DecodeString(partETag)
		if err != nil || len(binaryMD5) != 16 {
			logger.GetLogger("boulder").Errorf("invalid ETag format for part %d: %s", p.PartNumber, partETag)
			return nil, xhttp.ToError(xhttp.ErrInvalidPart)
		}
		hash.Write(binaryMD5)
		Chunks = append(Chunks, p.Chunks...)
		totalSize += p.Size
	}
	if totalSize > meta.MAX_OBJECT_SIZE {
		logger.GetLogger("boulder").Errorf("too large object for %s/%s", params.ObjKey, params.UploadID)
		return nil, xhttp.ToError(xhttp.ErrEntityTooLarge)
	}

	// 生成最终 ETag
	compositeMD5 := hex.EncodeToString(hash.Sum(nil))
	finalETag := fmt.Sprintf("%s-%d", compositeMD5, len(allParts))

	// 构造最终对象
	obj := &meta.Object{
		BaseObject: meta.BaseObject{
			Bucket:       params.BucketName,
			Key:          params.ObjKey,
			ETag:         meta.Etag(finalETag),
			Size:         totalSize,
			Chunks:       Chunks,
			LastModified: time.Now().UTC(),
			CreatedAt:    upload.Created, // 继承上传创建时间
			DataLocation: upload.DataLocation,
		},
		ContentType:        upload.ContentType,
		ContentEncoding:    upload.ContentEncoding,
		ContentLanguage:    upload.ContentLanguage,
		CacheControl:       upload.CacheControl,
		ContentDisposition: upload.ContentDisposition,
		StorageClass:       upload.StorageClass,
		UserMetadata:       upload.UserMetadata,
		Tags:               upload.Tags,
		Owner:              upload.Owner,
	}

	err = utils.RetryCall(3, func() error {
		txn, err := m.kvstore.BeginTxn(context.Background(), nil)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer func() {
			if txn != nil {
				_ = txn.Rollback()
			}
		}()

		// 保存对象
		objKey := "aws:object:" + ak.AccountID + ":" + obj.Bucket + "/" + obj.Key
		if err := txn.Set(objKey, obj); err != nil {
			logger.GetLogger("boulder").Errorf("failed to save object: %v", err)
			return fmt.Errorf("failed to save object: %w", err)
		} else {
			logger.GetLogger("boulder").Debugf("saved multi object: %s chunk %d", objKey, len(obj.Chunks))
		}

		// 循环删除 upload 及其所有 part（避免 DeletePrefix 限制）
		if err := txn.DeletePrefix(uploadKey, 0); err != nil { // 传 0 表示无限制或循环删除
			logger.GetLogger("boulder").Errorf("failed to delete upload info: %v", err)
			return fmt.Errorf("failed to delete upload info: %w", err)
		}

		//如果是覆盖已有的对象，要先删除旧的对象
		if existsOldObj {
			gckey := gc.GCChunkPrefix + utils.GenUUID()
			gcData := gc.GCChunk{
				GCData: gc.GCData{
					CreateAt: time.Now().UTC(),
					Items:    make([]gc.GCItem, 0),
				},
			}
			for _, id := range oldObj.Chunks {
				gcData.Items = append(gcData.Items, gc.GCItem{StorageID: oldObj.DataLocation, ID: id})
			}

			err = txn.Set(gckey, &gcData)
			if err != nil {
				logger.GetLogger("boulder").Errorf("%s/%s set gc chunk failed: %v", obj.Bucket, obj.Key, err)
				return fmt.Errorf("%s/%s set gc chunk failed: %w", obj.Bucket, obj.Key, err)
			}
		}

		// 提交事务
		err = txn.Commit()
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		txn = nil
		return nil
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed completed multipart upload bucket=%s, key=%s, uploadID=%s : %v", obj.Bucket, obj.Key, params.UploadID, err)
		return nil, fmt.Errorf("failed completed multipart upload bucket=%s, key=%s, uploadID=%s : %w", obj.Bucket, obj.Key, params.UploadID, err)
	}

	logger.GetLogger("boulder").Infof("completed multipart upload: bucket=%s, key=%s, uploadID=%s, parts=%d, size=%d",
		obj.Bucket, obj.Key, params.UploadID, len(cliParts), totalSize)

	return obj, nil
}

func (m *MultiPartService) ListParts(params *object.BaseObjectParams) (*meta.MultipartUpload, []*meta.PartObject, error) {
	// 验证访问密钥
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

	// 检查分段上传任务是否存在
	uploadKey := "aws:upload:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey + "/" + params.UploadID
	var upload meta.MultipartUpload
	exists, err := m.kvstore.Get(uploadKey, &upload)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get multipart upload: %v", err)
		return nil, nil, fmt.Errorf("failed to get multipart upload: %w", err)
	}
	if !exists {
		logger.GetLogger("boulder").Errorf("multipart upload not found: %s", uploadKey)
		return nil, nil, xhttp.ToError(xhttp.ErrNoSuchUpload)
	}

	// 开启事务
	txn, err := m.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 扫描所有 Part 元数据
	prefix := uploadKey + "/"
	var allParts []*meta.PartObject
	startKey := ""
	for {
		keys, nextKey, err := txn.Scan(prefix, startKey, 100)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to scan multipart upload parts: %v", err)
			return nil, nil, fmt.Errorf("failed to scan multipart upload parts: %w", err)
		}
		partBytes, err := txn.BatchGet(keys)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get multipart upload part meta: %v", err)
			return nil, nil, fmt.Errorf("failed to get multipart upload part meta: %w", err)
		}

		for k, data := range partBytes {
			logger.GetLogger("boulder").Infof("list get part  %s meta data %d", k, len(data))
			var part meta.PartObject
			if err := json.Unmarshal(data, &part); err != nil {
				logger.GetLogger("boulder").Errorf("failed to unmarshal part meta: %v", err)
				return nil, nil, fmt.Errorf("failed to unmarshal part meta: %w", err)
			}
			allParts = append(allParts, &part)
		}

		if len(keys) < 100 || nextKey == "" || int64(len(allParts)) > params.MaxParts {
			break
		}
		startKey = nextKey
	}

	// 按 PartNumber 排序
	sort.Slice(allParts, func(i, j int) bool {
		return allParts[i].PartNumber < allParts[j].PartNumber
	})

	return &upload, allParts, nil
}

func (m *MultiPartService) ListMultipartUploads(params *object.BaseObjectParams) ([]*meta.MultipartUpload, error) {
	// 验证访问密钥
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

	// 检查 bucket 是否存在
	bucketKey := "aws:bucket:" + ak.AccountID + ":" + params.BucketName
	var bucketMeta meta.BucketMetadata
	bucketExists, err := m.kvstore.Get(bucketKey, &bucketMeta)
	if err != nil || !bucketExists {
		logger.GetLogger("boulder").Errorf("bucket %s does not exist or access denied", params.BucketName)
		return nil, xhttp.ToError(xhttp.ErrNoSuchBucket)
	}

	// 构建扫描前缀：aws:upload:<account>:<bucket>/
	prefix := "aws:upload:" + ak.AccountID + ":" + params.BucketName + "/"

	// 如果有 Prefix 参数，附加到前缀后
	searchPrefix := prefix
	if params.Prefix != "" {
		searchPrefix += params.Prefix
	}

	// 开始事务
	txn, err := m.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	var uploads []*meta.MultipartUpload
	startKey := ""

	// 处理 Marker：如果提供了 KeyMarker 和 UploadIDMarker，设置起始扫描位置
	if params.KeyMarker != "" && params.UploadIDMarker != "" {
		startKey = prefix + params.KeyMarker + "/" + params.UploadIDMarker
		// 注意：这个 key 应该正好是 upload 元数据的 key
	}

	// 扫描循环
	for {
		keys, nextKey, err := txn.Scan(searchPrefix, startKey, 100)
		logger.GetLogger("boulder").Infof("list multipart uploads keys %v, nextkey : %s", keys, nextKey)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to scan multipart uploads: %v", err)
			return nil, fmt.Errorf("failed to scan multipart uploads: %w", err)
		}

		if len(keys) == 0 {
			break
		}

		// 批量获取数据
		data, err := txn.BatchGet(keys)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to batch get upload metadata: %v", err)
			return nil, fmt.Errorf("failed to batch get upload metadata: %w", err)
		}

		// 遍历每个 key-value
		for key, raw := range data {
			// 内置判断：是否为 upload 元数据（不是 part）
			// 格式：aws:upload:account:bucket/key/uploadID
			// part 格式：aws:upload:account:bucket/key/uploadID/xxxx
			// 所以我们检查：在 prefix 之后，是否只有两段（key 和 uploadID）
			relKey := strings.TrimPrefix(key, prefix)
			if relKey == key { // 不以 prefix 开头，跳过
				logger.GetLogger("boulder").Infof("scan multipart uploads key %s relKey %s", key, relKey)
				continue
			}

			// 分割相对路径
			parts := strings.Split(relKey, "/")
			// uploadid 是以 u0118-开头
			if len(parts) >= 2 && strings.HasPrefix(parts[len(parts)-1], UID_PREFIX+"-") {
				// pass
			} else {
				logger.GetLogger("boulder").Infof("key is a not a upload id: %#v", parts)
				continue // 跳过 part 或无效格式
			}

			// 是 upload 元数据，尝试反序列化
			var upload meta.MultipartUpload
			if err := json.Unmarshal(raw, &upload); err != nil {
				logger.GetLogger("boulder").Infof("failed to unmarshal upload %s: %v", key, err)
				continue
			}

			// 再次确认 Key 匹配（防止 prefix 匹配错误）
			if params.Prefix != "" && !strings.HasPrefix(upload.Key, params.Prefix) {
				logger.GetLogger("boulder").Infof("failed to match multipart upload  %s, %s", params.Prefix, upload.Key)
				continue
			}

			uploads = append(uploads, &upload)

			// 提前截断：如果已经获取了 MaxUploads + 1 个，用于判断 IsTruncated
			if int64(len(uploads)) >= params.MaxUploads+1 {
				break
			}
		}

		// 提前退出条件
		if int64(len(uploads)) >= params.MaxUploads+1 || nextKey == "" || len(keys) < 100 {
			break
		}

		startKey = nextKey
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = nil
	logger.GetLogger("boulder").Infof("list multipart uploads  %#v", uploads)
	return uploads, nil
}
