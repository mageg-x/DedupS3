package object

import (
	"context"
	"errors"
	"fmt"
	"github.com/mageg-x/boulder/service/chunk"
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

	// 检查bucket是否存在
	key := "aws:bucket:" + ak.AccountID + ":" + params.BucketName
	var bucket *meta.BucketMetadata
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		data, ok, e := cache.Get(context.Background(), key)
		if e == nil && ok {
			_bucket, yes := data.(*meta.BucketMetadata)
			if yes {
				bucket = _bucket
			} else {
				// 缓存的数据类型错误，删除缓存
			}
		}
	}
	if bucket == nil {
		var _bucket meta.BucketMetadata
		exist, err := o.kvstore.Get(key, &_bucket)
		if !exist || err != nil {
			logger.GetLogger("boulder").Errorf("bucket %s does not exist", key)
			return nil, xhttp.ToError(xhttp.ErrNoSuchBucket)
		}
		bucket = &_bucket
		// 写入cache
		if cache, e := xcache.GetCache(); e == nil && cache != nil {
			cache.Set(context.Background(), key, bucket, time.Second*600)
		}
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
	err = chunker.DoChunk(r, objectInfo)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to chunk object: %v", err)
	}

	return objectInfo, err
}
