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

// GetBucketLocationResult 符合S3标准的存储桶位置响应结构
// 注意：Location字段使用了",chardata"标签，这是Go XML包的特殊语法，表示该字段值将作为XML元素的字符数据
// 当Location字段不为空时，生成的XML格式为：<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">区域名称</LocationConstraint>
// 当Location字段为空时，生成的XML格式为：<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>
// 这符合AWS S3 API的官方规范 - 对于默认区域（us-east-1），LocationConstraint元素应为空
type GetBucketLocationResult struct {
	XMLName  xml.Name `xml:"LocationConstraint"`
	XMLNS    string   `xml:"xmlns,attr"` // 固定值为http://s3.amazonaws.com/doc/2006-03-01/
	Location string   `xml:",chardata"`
}

// PolicyStatus 表示桶策略状态的XML响应结构
type PolicyStatus struct {
	XMLName  xml.Name `xml:"IsPublic" json:",omitempty"`
	IsPublic bool     `xml:",chardata" json:"IsPublic"`
}

// GetBucketPolicyStatusResult 符合AWS S3规范的策略状态响应结构
type GetBucketPolicyStatusResult struct {
	XMLName      xml.Name     `xml:"GetBucketPolicyStatusResult" json:",omitempty"`
	XMLNS        string       `xml:"xmlns,attr" json:",omitempty"` // 设置AWS S3 XML命名空间
	PolicyStatus PolicyStatus `xml:"PolicyStatus" json:"PolicyStatus"`
}

type BaseBucketParams struct {
	BucketName        string
	Location          string
	ObjectLockEnabled bool
	AccessKeyID       string
	ExpectedOwnerID   string
	Tags              []meta.Tag
	ObjectLockToken   string
	ContentMD5        string
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

	txn, err := b.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
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

func (b *BucketService) PutBucketTagging(params *BaseBucketParams) error {
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

	if params.ExpectedOwnerID != "" {
		// 检查存储桶的实际所有者是否与请求的所有者匹配
		if bucket.Owner.ID != params.ExpectedOwnerID {
			logger.GetLogger("boulder").Errorf("bucket owner mismatch: expected %s, got %s", params.ExpectedOwnerID, bucket.Owner.ID)
			return xhttp.ToError(xhttp.ErrAccessDenied)
		}
	}
	// 设置标签
	currentTime := time.Now().UTC()
	bucket.Tagging = &meta.Tagging{
		XMLName:   xml.Name{Local: "Tagging"},
		XMLNS:     "http://s3.amazonaws.com/doc/2006-03-01/",
		CreatedAt: currentTime,
		UpdatedAt: currentTime,
		TagSet: meta.TagSet{
			Tags: params.Tags,
		},
	}

	err = txn.Set(bucketKey, &bucket)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to set bucket tags: %v", err)
		return fmt.Errorf("failed to set bucket tags: %v", err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	txn = nil

	// 清除缓存
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		_ = cache.Del(context.Background(), bucketKey)
	}

	return nil
}

// PutBucketLifecycle 设置存储桶的生命周期配置
func (b *BucketService) PutBucketLifecycle(params *BaseBucketParams, lifecycle *meta.LifecycleConfiguration) error {
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

	if params.ExpectedOwnerID != "" {
		// 检查存储桶的实际所有者是否与请求的所有者匹配
		if bucket.Owner.ID != params.ExpectedOwnerID {
			logger.GetLogger("boulder").Errorf("bucket owner mismatch: expected %s, got %s", params.ExpectedOwnerID, bucket.Owner.ID)
			return xhttp.ToError(xhttp.ErrAccessDenied)
		}
	}
	// 设置生命周期配置
	currentTime := time.Now().UTC()
	if lifecycle != nil {
		// 设置XML命名空间
		if lifecycle.XMLNS == "" {
			lifecycle.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
		}
		lifecycle.CreatedAt = currentTime
		lifecycle.UpdatedAt = currentTime
		bucket.Lifecycle = lifecycle
	} else {
		// 如果传入nil，则清除生命周期配置
		bucket.Lifecycle = nil
	}

	err = txn.Set(bucketKey, &bucket)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to set bucket lifecycle configuration: %v", err)
		return fmt.Errorf("failed to set bucket lifecycle configuration: %v", err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	txn = nil

	// 清除缓存
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		_ = cache.Del(context.Background(), bucketKey)
	}

	logger.GetLogger("boulder").Tracef("successfully set lifecycle configuration for bucket: %s", params.BucketName)
	return nil
}

// PutBucketNotification 设置存储桶的事件通知配置
func (b *BucketService) PutBucketNotification(params *BaseBucketParams, notification *meta.EventNotificationConfiguration) error {
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

	if params.ExpectedOwnerID != "" {
		// 检查存储桶的实际所有者是否与请求的所有者匹配
		if bucket.Owner.ID != params.ExpectedOwnerID {
			logger.GetLogger("boulder").Errorf("bucket owner mismatch: expected %s, got %s", params.ExpectedOwnerID, bucket.Owner.ID)
			return xhttp.ToError(xhttp.ErrAccessDenied)
		}
	}
	// 设置事件通知配置
	currentTime := time.Now().UTC()
	if notification != nil {
		// 设置XML命名空间
		if notification.XMLNS == "" {
			notification.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
		}
		notification.CreatedAt = currentTime
		notification.UpdatedAt = currentTime
		bucket.Notification = notification
	} else {
		// 如果传入nil，则清除事件通知配置
		bucket.Notification = nil
	}

	err = txn.Set(bucketKey, &bucket)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to set bucket notification configuration: %v", err)
		return fmt.Errorf("failed to set bucket notification configuration: %v", err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	txn = nil

	// 清除缓存
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		_ = cache.Del(context.Background(), bucketKey)
	}

	logger.GetLogger("boulder").Tracef("successfully set notification configuration for bucket: %s", params.BucketName)
	return nil
}

// PutBucketObjectLockConfig 设置存储桶的对象锁定配置
func (b *BucketService) PutBucketObjectLockConfig(params *BaseBucketParams, config *meta.ObjectLockConfiguration) error {
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

	if params.ExpectedOwnerID != "" {
		// 检查存储桶的实际所有者是否与请求的所有者匹配
		if bucket.Owner.ID != params.ExpectedOwnerID {
			logger.GetLogger("boulder").Errorf("bucket owner mismatch: expected %s, got %s", params.ExpectedOwnerID, bucket.Owner.ID)
			return xhttp.ToError(xhttp.ErrAccessDenied)
		}
	}

	// 设置对象锁定配置
	currentTime := time.Now().UTC()
	if config != nil {
		// 设置XML命名空间
		if config.XMLNS == "" {
			config.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
		}
		config.CreatedAt = currentTime
		config.UpdatedAt = currentTime

	}
	bucket.ObjectLock = config

	err = txn.Set(bucketKey, &bucket)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to set bucket object lock configuration: %v", err)
		return fmt.Errorf("failed to set bucket object lock configuration: %v", err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	txn = nil

	// 清除缓存
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		_ = cache.Del(context.Background(), bucketKey)
	}

	logger.GetLogger("boulder").Tracef("successfully set object lock configuration for bucket: %s", params.BucketName)
	return nil
}

// PutBucketACL 设置存储桶的访问控制列表
func (b *BucketService) PutBucketACL(params *BaseBucketParams, acl *meta.AccessControlPolicy) error {
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

	if params.ExpectedOwnerID != "" {
		// 检查存储桶的实际所有者是否与请求的所有者匹配
		if bucket.Owner.ID != params.ExpectedOwnerID {
			logger.GetLogger("boulder").Errorf("bucket owner mismatch: expected %s, got %s", params.ExpectedOwnerID, bucket.Owner.ID)
			return xhttp.ToError(xhttp.ErrAccessDenied)
		}
	}
	// 设置访问控制策略
	if acl != nil {
		// 设置XML命名空间
		if acl.XMLNS == "" {
			acl.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
		}
		// 确保所有者信息正确
		acl.Owner = meta.CanonicalUser{
			ID:          bucket.Owner.ID,
			DisplayName: bucket.Owner.DisplayName,
		}
		bucket.ACL = acl
	} else {
		// 如果传入nil，则清除ACL配置
		bucket.ACL = nil
	}

	err = txn.Set(bucketKey, &bucket)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to set bucket ACL: %v", err)
		return fmt.Errorf("failed to set bucket ACL: %v", err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	txn = nil

	// 清除缓存
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		_ = cache.Del(context.Background(), bucketKey)
	}

	logger.GetLogger("boulder").Tracef("successfully set ACL for bucket: %s", params.BucketName)
	return nil
}

func (b *BucketService) PutBucketPolicy(params *BaseBucketParams, policy *meta.BucketPolicy) error {
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

	if params.ExpectedOwnerID != "" {
		// 检查存储桶的实际所有者是否与请求的所有者匹配
		if bucket.Owner.ID != params.ExpectedOwnerID {
			logger.GetLogger("boulder").Errorf("bucket owner mismatch: expected %s, got %s", params.ExpectedOwnerID, bucket.Owner.ID)
			return xhttp.ToError(xhttp.ErrAccessDenied)
		}
	}
	if policy != nil {
		currentTime := time.Now().UTC()
		policy.CreatedAt = currentTime
		policy.UpdatedAt = currentTime
	}
	bucket.Policy = policy
	err = txn.Set(bucketKey, &bucket)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to set bucket Policy: %v", err)
		return fmt.Errorf("failed to set bucket Policy: %v", err)
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	txn = nil

	// 清除缓存
	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		_ = cache.Del(context.Background(), bucketKey)
	}

	logger.GetLogger("boulder").Tracef("successfully set Policy for bucket: %s", params.BucketName)
	return nil
}
