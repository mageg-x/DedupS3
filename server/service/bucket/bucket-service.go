package bucket

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	xhttp "github.com/mageg-x/boulder/internal/http"
	"sync"
	"time"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/meta"
	"github.com/mageg-x/boulder/service/iam"
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

type CreateBucketParams struct {
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

func (b *BucketService) CreateBucket(params CreateBucketParams) error {
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

	// 先判断bucket 是否已经存在
	key := "aws:bucket:" + params.BucketName
	var bucket meta.BucketMetadata
	exist, err := b.kvstore.Get(context.Background(), key, &bucket)
	if exist && err == nil {
		if bucket.Owner.ID == u.ID {
			logger.GetLogger("boulder").Errorf("bucket %s already owned by you", params.BucketName)
			return xhttp.ToError(xhttp.ErrBucketAlreadyOwnedByYou)
		}
		logger.GetLogger("boulder").Errorf("bucket %s already exists", params.BucketName)
		return xhttp.ToError(xhttp.ErrBucketAlreadyExists)
	}

	bm := meta.BucketMetadata{
		Name:         params.BucketName,
		CreationDate: time.Now().UTC(),
		Owner:        meta.CanonicalUser{ID: u.ID, DisplayName: u.Username},
		Location:     params.Location,
	}

	err = b.kvstore.Put(context.Background(), key, &bm)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to put bucket: %v", err)
		return fmt.Errorf("failed to put bucket: %v", err)
	}

	return nil
}
