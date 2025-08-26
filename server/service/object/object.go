package object

import (
	"context"
	"errors"
	"fmt"
	"github.com/mageg-x/boulder/service/chunk"
	"io"
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
	Etag         string
	StorageClass string
	ContentLen   int64
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

func (o *ObjectService) PutObject(r io.Reader, params BaseObjectParams) error {
	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		return errors.New("failed to get iam service")
	}

	ak, err := iamService.GetAccessKey(params.AccessKeyID)
	if err != nil || ak == nil {
		logger.GetLogger("boulder").Errorf("failed to get access key %s", params.AccessKeyID)
		return fmt.Errorf("failed to get access key %s", params.AccessKeyID)
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
			return xhttp.ToError(xhttp.ErrNoSuchBucket)
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
		return fmt.Errorf("failed to get storage service")
	}

	scs := bs.GetStoragesByClass(storageClass)
	if len(scs) == 0 {
		logger.GetLogger("boulder").Errorf("no storage class %s", storageClass)
		return fmt.Errorf("no storage class %s", storageClass)
	}
	sc := scs[0]

	objectInfo := meta.NewObject(params.BucketName, params.ObjKey)
	objectInfo.ETag = params.Etag
	objectInfo.StorageClass = storageClass
	objectInfo.DataLocation = sc.ID
	objectInfo.Size = params.ContentLen
	objectInfo.Owner = meta.Owner{
		ID:          ak.AccountID,
		DisplayName: ak.Username,
	}

	// 进行chunk切分
	chunker := chunk.GetChunkService()
	if chunker == nil {
		logger.GetLogger("boulder").Errorf("failed to get chunk service")
		return fmt.Errorf("failed to get chunk service")
	}
	chunker.DoChunk(r, params.BucketName, params.ObjKey)

	return nil
}
