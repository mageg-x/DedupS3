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
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mageg-x/boulder/service/chunk"
	"github.com/mageg-x/boulder/service/storage"

	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
	"github.com/mageg-x/boulder/service/iam"
	"github.com/mageg-x/boulder/service/object"
	"github.com/rs/xid"
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
	XMLNS    string   `xml:"xmlns,attr"` // 命名空间
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"` // 命名空间
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`

	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
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

func (m *MultiPartService) WritePartMeta(cs *chunk.ChunkService, chunks []*meta.Chunk, blocks map[string]*meta.Block, object *meta.BaseObject) error {
	var txErr error
	maxRetry := 3
	part := meta.BaseObjectToPart(object)

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
			newBlock := v.Clone()
			bakBlocks[k] = newBlock
		}

		bakPart := part.Clone()
		objPrefix := "aws:upload:"
		objSuffix := fmt.Sprintf("/%s/%d", part.UploadID, part.PartNumber)
		txErr = cs.WriteMeta(context.Background(), part.Owner.ID, bakAllChunks, bakBlocks, meta.PartToBaseObject(bakPart), objPrefix, objSuffix)
		if txErr == nil {
			break
		} else if errors.Is(txErr, kv.ErrTxnCommit) && i < maxRetry-1 {
			// 事务提交冲突
			logger.GetLogger("boulder").Warnf("transmission write object %s/%s commit failed: %v, and  retry %d times", part.Bucket, part.Key, txErr, i+1)
			baseDelay := 500 * time.Millisecond
			jitter := time.Duration(rand.Int63n(100)) * time.Millisecond
			sleep := baseDelay<<uint(i) + jitter
			time.Sleep(sleep)
		} else {
			logger.GetLogger("boulder").Errorf("transmission write object %s/%s  meta info failed: %v，retry times %d", part.Bucket, part.Key, txErr, i+1)
		}
	}
	return txErr
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
	ifNoneMatch := headers.Get(xhttp.IfNoneMatch)
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
		if !objOK || _object.ETag != strings.Trim(ifMatch, `"`) {
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
			if objOK && _object.ETag == strings.Trim(ifNoneMatch, `"`) {
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
	uploadID := xid.New().String()
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
		return nil, fmt.Errorf("failed to begin transaction for multi upload %s", key)
	}
	defer txn.Rollback()
	err = txn.Set(key, &upload)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to set multi upload meta %s uploadid %s ", key, uploadID)
		return nil, fmt.Errorf("failed to set multi upload meta %s uploadid %s ", key, uploadID)
	}
	err = txn.Commit()
	if err != nil {
		logger.GetLogger("boulder").Errorf("create multi upload %s uploadid %s  failed to commit transaction", key, uploadID)
		return nil, fmt.Errorf("create multi upload %s uploadid %s failed to commit transaction", key, uploadID)
	}
	return upload, nil
}

func (m *MultiPartService) UploadPart(r io.Reader, params *object.BaseObjectParams) (*meta.PartObject, error) {
	// 验证参数
	if params == nil || params.UploadID == "" || params.PartNumber <= 0 || r == nil {
		logger.GetLogger("boulder").Errorf("invalid parameters for UploadPart")
		return nil, xhttp.ToError(xhttp.ErrInvalidQueryParams)
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
			Key:          params.ObjKey,
			ETag:         params.ContentMd5,
			Size:         params.ContentLen,
			LastModified: time.Now().UTC(),
			CreatedAt:    time.Now().UTC(),
			Chunks:       make([]string, 0),
		},
		UploadID:   params.UploadID,
		PartNumber: int(params.PartNumber),
		Owner:      upload.Owner,
	}

	// 进行chunk切分
	chunker := chunk.GetChunkService()
	if chunker == nil {
		logger.GetLogger("boulder").Errorf("failed to get chunk service")
		return nil, fmt.Errorf("failed to get chunk service")
	}

	err = chunker.DoChunk(r, meta.PartToBaseObject(part), m.WritePartMeta)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to chunk object: %v", err)
	}
	return nil, nil
}

func (m *MultiPartService) CompleteMultipartUpload(parts []meta.PartETag, params *object.BaseObjectParams) (*meta.Object, error) {
	// 验证参数
	if params.AccessKeyID == "" || params.BucketName == "" || params.ObjKey == "" || params.UploadID == "" || len(parts) == 0 {
		logger.GetLogger("boulder").Errorf("invalid parameters for CompleteMultipartUpload")
		return nil, xhttp.ToError(xhttp.ErrInvalidQueryParams)
	}

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

	// 检查uploadId任务是否存在
	uploadKey := "aws:upload:" + ak.AccountID + ":" + params.BucketName + "/" + params.ObjKey + "/" + params.UploadID
	var upload meta.MultipartUpload
	exists, err := m.kvstore.Get(uploadKey, &upload)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get multipart upload: %v", err)
		return nil, fmt.Errorf("failed to get multipart upload: %v", err)
	}
	if !exists {
		logger.GetLogger("boulder").Errorf("multipart upload not found: %s", uploadKey)
		return nil, xhttp.ToError(xhttp.ErrNoSuchUpload)
	}

	txn, err := m.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	prefix := uploadKey + "/"
	var allParts []*meta.PartObject
	startKey := ""
	for {
		keys, nextkey, err := txn.Scan(prefix, startKey, 100)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to scan multipart upload %s part meta: %v", prefix, err)
			return nil, fmt.Errorf("failed to scan multipart upload %s part meta: %v", prefix, err)
		}
		_parts, err := txn.BatchGet(keys)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to get multipart upload %s part meta: %v", prefix, err)
			return nil, fmt.Errorf("failed to get multipart upload %s part meta: %v", prefix, err)
		}
		if len(_parts) != len(keys) {
			logger.GetLogger("boulder").Errorf("multipart upload %s part meta: unexpected number of parts %d:%d", prefix, len(_parts), len(keys))
		}
		for _, _p := range _parts {
			_part := meta.PartObject{}
			err := json.Unmarshal(_p, &_part)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to unmarshal part meta: %v", err)
				return nil, fmt.Errorf("failed to unmarshal part meta: %v", err)
			}
			allParts = append(allParts, &_part)
		}
		if len(keys) < 100 || nextkey == "" {
			break
		}
		startKey = nextkey
	}
	// 按PartNumber排序
	sort.Slice(allParts, func(i, j int) bool {
		return allParts[i].PartNumber < allParts[j].PartNumber
	})

	partMap := make(map[int]meta.PartETag)
	for _, p := range parts {
		partMap[p.PartNumber] = p
	}

	// 验证分段顺序和ETag
	totalSize := int64(0)
	hash := md5.New()
	Chunks := make([]string, 0, len(allParts))
	for i, p := range allParts {
		if i > 0 && p.PartNumber <= allParts[i-1].PartNumber {
			logger.GetLogger("boulder").Errorf("uploadid  %s get invalid part num order", params.UploadID)
			return nil, xhttp.ToError(xhttp.ErrInvalidPartOrder)
		}
		// 检查分段是否存在
		_part, exists := partMap[p.PartNumber]
		if !exists {
			logger.GetLogger("boulder").Errorf("part %d not found in upload %s", p.PartNumber, params.UploadID)
			return nil, xhttp.ToError(xhttp.ErrInvalidPart)
		}

		// 验证ETag是否匹配
		if _part.ETag != p.ETag {
			logger.GetLogger("boulder").Errorf("ETag mismatch for part %d: expected %s, got %s", p.PartNumber, _part.ETag, p.ETag)
			return nil, xhttp.ToError(xhttp.ErrInvalidPart)
		}
		Chunks = append(Chunks, p.Chunks...)
		totalSize += p.Size
		hash.Write([]byte(p.ETag))
	}

	// 生成完整对象的ETag
	compositeMD5 := hex.EncodeToString(hash.Sum(nil))
	finalETag := fmt.Sprintf("%s-%d", compositeMD5, len(parts))
	obj := &meta.Object{
		BaseObject: meta.BaseObject{
			Bucket:       params.BucketName,
			Key:          params.ObjKey,
			ETag:         finalETag,
			Size:         totalSize,
			Chunks:       Chunks,
			LastModified: time.Now().UTC(),
			CreatedAt:    time.Now().UTC(),
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

	// 保存对象元数据
	objKey := "aws:object:" + ak.AccountID + ":" + obj.Bucket + "/" + obj.Key

	// 保存对象信息
	err = txn.Set(objKey, obj)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to save object: %v", err)
		return nil, fmt.Errorf("failed to save object: %v", err)
	}

	// 删除上传任务信息
	err = txn.DeletePrefix(uploadKey, 10000)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete upload info: %v", err)
		return nil, fmt.Errorf("failed to delete upload info: %v", err)
	}

	// 提交事务
	err = txn.Commit()
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
	}

	logger.GetLogger("boulder").Infof("completed multipart upload: bucket=%s, key=%s, uploadID=%s, parts=%d, size=%d object %#v",
		obj.Bucket, obj.Key, params.UploadID, len(parts), totalSize, obj)

	return obj, nil
}
