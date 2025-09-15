package bucket

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"sync"
	"time"

	xhttp "github.com/mageg-x/boulder/internal/http"
	xcache "github.com/mageg-x/boulder/internal/storage/cache"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/meta"
	"github.com/mageg-x/boulder/service/iam"
)

const (
	MAX_BUCKET_NUM = 1000
)

var (
	instance *BucketService
	mu       = sync.Mutex{}
)

type BucketService struct {
	kvstore kv.KVStore
}

type CreateBucketLocationConfiguration struct {
	XMLName  xml.Name `xml:"CreateBucketConfiguration" json:"-"`
	Location string   `xml:"LocationConstraint"`
}

type BaseBucketParams struct {
	BucketName        string
	Location          string
	ObjectLockEnabled bool
	AccessKeyID       string
}

func GetBucketService() *BucketService {
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
	instance = &BucketService{
		kvstore: store,
	}
	return instance
}

func (b *BucketService) CreateBucket(params *BaseBucketParams) error {
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
	ac, err := iamService.GetAccount(ak.AccountID)
	if err != nil || ac == nil {
		logger.GetLogger("boulder").Errorf("failed to get account %s", ak.AccountID)
		return fmt.Errorf("failed to get account %s", ak.AccountID)
	}
	u, err := ac.GetUser(ak.Username)
	if err != nil || u == nil {
		logger.GetLogger("boulder").Errorf("failed to get user %s", ak.Username)
		return fmt.Errorf("failed to get user %s", ak.Username)
	}

	txn, err := b.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %v", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 先判断bucket 是否已经存在
	key := "aws:bucket:" + ac.AccountID + ":" + params.BucketName
	var bucket meta.BucketMetadata
	exist, err := txn.Get(key, &bucket)
	if exist && err == nil {
		if bucket.Owner.ID == ac.AccountID {
			logger.GetLogger("boulder").Warnf("bucket %s already owned by you", params.BucketName)
			return xhttp.ToError(xhttp.ErrBucketAlreadyOwnedByYou)
		}
		logger.GetLogger("boulder").Warnf("bucket %s already exists", params.BucketName)
		return xhttp.ToError(xhttp.ErrBucketAlreadyExists)
	}

	bm := meta.BucketMetadata{
		Name:         params.BucketName,
		CreationDate: time.Now().UTC(),
		Owner:        meta.Owner{ID: ac.AccountID, DisplayName: ac.Name},
		Location:     params.Location,
	}

	err = txn.Set(key, &bm)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to put bucket: %v", err)
		return fmt.Errorf("failed to put bucket: %v", err)
	}

	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	txn = nil

	// 删除cache
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		cache.Del(context.Background(), key)
	}
	return nil
}

func (b *BucketService) GetBucketInfo(params *BaseBucketParams) (*meta.BucketMetadata, error) {
	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		return nil, errors.New("failed to get iam service")
	}

	ak, err := iamService.GetAccessKey(params.AccessKeyID)
	if err != nil || ak == nil {
		logger.GetLogger("boulder").Errorf("failed to get access key %s", params.AccessKeyID)
		return nil, fmt.Errorf("failed to get access key %s", params.AccessKeyID)
	}

	key := "aws:bucket:" + ak.AccountID + ":" + params.BucketName

	// 先从cache 中查找
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		data, ok, e := cache.Get(context.Background(), key)
		if e == nil && ok {
			_bucket, yes := data.(*meta.BucketMetadata)
			if yes {
				logger.GetLogger("boulder").Tracef("get bucket %s from cache", params.BucketName)
				return _bucket, nil
			} else {
				logger.GetLogger("boulder").Errorf("failed to get bucket %s metadata from cache", params.BucketName)
				cache.Del(context.Background(), key)
			}
		}
	}

	var bucket meta.BucketMetadata
	exist, err := b.kvstore.Get(key, &bucket)
	if !exist || err != nil {
		logger.GetLogger("boulder").Errorf("bucket %s does not exist", params.BucketName)
		return nil, xhttp.ToError(xhttp.ErrNoSuchBucket)
	}

	// 写入cache
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		cache.Set(context.Background(), key, &bucket, time.Second*600)
	}
	return &bucket, nil
}

func (b *BucketService) ListBuckets(params *BaseBucketParams) ([]*meta.BucketMetadata, *meta.Owner, error) {
	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		return nil, nil, errors.New("failed to get iam service")
	}

	ak, err := iamService.GetAccessKey(params.AccessKeyID)
	if err != nil || ak == nil {
		logger.GetLogger("boulder").Errorf("failed to get access key %s", params.AccessKeyID)
		return nil, nil, fmt.Errorf("failed to get access key %s", params.AccessKeyID)
	}
	ac, err := iamService.GetAccount(ak.AccountID)
	if err != nil || ac == nil {
		logger.GetLogger("boulder").Errorf("failed to get account %s", ak.AccountID)
		return nil, nil, fmt.Errorf("failed to get account %s", ak.AccountID)
	}
	owner := meta.Owner{
		ID:          ac.AccountID,
		DisplayName: ac.Name,
	}

	txn, err := b.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, nil, fmt.Errorf("failed to initialize kvstore txn: %v", err)
	}

	defer txn.Rollback()

	prefix := "aws:bucket:" + ak.AccountID + ":"
	buckets, _, err := txn.Scan(prefix, "", MAX_BUCKET_NUM)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to list buckets: %v", err)
		return nil, nil, fmt.Errorf("failed to list buckets: %v", err)
	}

	logger.GetLogger("boulder").Tracef("list buckets: %v", buckets)

	var allBuckets []*meta.BucketMetadata
	for _, name := range buckets {
		bucket := &meta.BucketMetadata{}
		exist, err := txn.Get(name, &bucket)
		if err != nil || !exist {
			logger.GetLogger("boulder").Errorf("failed to get bucket %s: %v", name, err)
			continue
		}
		allBuckets = append(allBuckets, bucket)
	}

	return allBuckets, &owner, nil
}
func (b *BucketService) DeleteBucket(params *BaseBucketParams) error {
	// 获取IAM服务
	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		return errors.New("failed to get iam service")
	}

	// 验证访问密钥
	ak, err := iamService.GetAccessKey(params.AccessKeyID)
	if err != nil || ak == nil {
		logger.GetLogger("boulder").Errorf("failed to get access key %s", params.AccessKeyID)
		return xhttp.ToError(xhttp.ErrAccessDenied)
	}

	ac, err := iamService.GetAccount(ak.AccountID)
	if err != nil || ac == nil {
		logger.GetLogger("boulder").Errorf("failed to get account %s", ak.AccountID)
		return xhttp.ToError(xhttp.ErrAccessDenied)
	}

	// 构建存储桶key
	bucketKey := "aws:bucket:" + ak.AccountID + ":" + params.BucketName

	// 开始事务
	txn, err := b.kvstore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %v", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 检查存储桶是否存在
	var bucket meta.BucketMetadata
	exist, err := txn.Get(bucketKey, &bucket)
	if !exist || err != nil {
		logger.GetLogger("boulder").Errorf("bucket %s does not exist", params.BucketName)
		return xhttp.ToError(xhttp.ErrNoSuchBucket)
	}

	// 检查用户是否是存储桶所有者
	if bucket.Owner.ID != ac.AccountID {
		logger.GetLogger("boulder").Errorf("access denied: user %s :%s is not the owner of bucket %s", ac.AccountID, bucket.Owner.ID, params.BucketName)
		return xhttp.ToError(xhttp.ErrAccessDenied)
	}

	// 检查存储桶是否为空（检查是否有对象）
	// 构建对象键前缀
	objectPrefix := "aws:object:" + ac.AccountID + ":" + params.BucketName + "/"
	objects, _, err := txn.Scan(objectPrefix, "", 1)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to scan objects in bucket %s: %v", params.BucketName, err)
		return fmt.Errorf("failed to scan objects: %v", err)
	}

	// 检查是否有分段上传任务
	multipartPrefix := "aws:object:" + ak.AccountID + ":" + params.BucketName + "/"
	multiparts, _, err := txn.Scan(multipartPrefix, "", 1)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to scan multipart uploads in bucket %s: %v", params.BucketName, err)
		return fmt.Errorf("failed to scan multipart uploads: %v", err)
	}

	if len(objects) > 0 || len(multiparts) > 0 {
		logger.GetLogger("boulder").Tracef("bucket %s not empty", params.BucketName)
		return xhttp.ToError(xhttp.ErrBucketNotEmpty)
	}

	// 删除存储桶元数据
	if err := txn.Delete(bucketKey); err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete bucket %s metadata: %v", params.BucketName, err)
		return fmt.Errorf("failed to delete bucket metadata: %v", err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	txn = nil

	// 清除缓存
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		cache.Del(context.Background(), bucketKey)
	}

	logger.GetLogger("boulder").Tracef("successfully deleted bucket: %s", params.BucketName)
	return nil
}
