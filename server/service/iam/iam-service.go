package iam

import (
	"context"
	"errors"
	"fmt"

	"sync"
	"time"

	"github.com/mageg-x/boulder/internal/logger"
	xcache "github.com/mageg-x/boulder/internal/storage/cache"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
)

var (
	instance *IamService = nil
	mu                   = sync.Mutex{}
)

// IamService 提供IAM账户管理功能
type IamService struct {
	iam kv.KVStore
}

func GetIamService() *IamService {
	mu.Lock()
	defer mu.Unlock()

	if instance == nil || instance.iam == nil {
		kvStore, err := kv.GetKvStore()
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to get kv store: %v", err)
			return nil
		}
		instance = &IamService{
			iam: kvStore,
		}
	}

	if instance == nil || instance.iam == nil {
		return nil
	}
	return instance
}

// CreateAccount 创建新的IAM账户
// 根据AWS标准，账户ID由系统自动生成，用户提供用户名和密码
func (s *IamService) CreateAccount(username, password string) (*meta.IAMAccount, error) {
	if err := meta.ValidateUsername(username); err != nil {
		logger.GetLogger("boulder").Errorf("username %s is invalid format", username)
		return nil, errors.New("invalid username format")
	}
	if err := meta.ValidatePassword(password, username); err != nil {
		logger.GetLogger("boulder").Errorf("password for user %s is invalid: %v", username, err)
		return nil, fmt.Errorf("invalid password: %w", err)
	}

	accountID := meta.GenerateAccountID(username)
	key := "aws:iam:account:id:" + accountID

	ctx := context.Background()
	// 检查账户是否已存在
	_, exists, err := s.iam.GetRaw(ctx, key)
	if exists {
		logger.GetLogger("boulder").Errorf("username %s already exists", username)
		return nil, errors.New("account already exists")
	}

	// 创建新账户
	account := meta.CreateAccount(username)

	// 创建根用户
	rootUser, err := account.CreateRootUser(username, password)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to create root user for account %s: %v", account.AccountID, err)
		return nil, err
	}

	if err = s.iam.Put(ctx, key, account); err != nil {
		logger.GetLogger("boulder").Errorf("failed to store account %s data in transaction: %v", account.AccountID, err)
		return nil, err
	}

	// 更新 access key 索引
	key = "aws:iam:account:ak:" + rootUser.AccessKeys[0].AccessKeyID
	if err = s.iam.Put(ctx, key, &rootUser.AccessKeys[0]); err != nil {
		logger.GetLogger("boulder").Errorf("failed to store account %s access key in transaction: %v", account.AccountID, err)
		return nil, err
	}

	logger.GetLogger("boulder").Infof("account %s created with user %s", account.AccountID, username)
	return account, nil
}

// GetAccount 获取IAM账户
func (s *IamService) GetAccount(accountID string) (*meta.IAMAccount, error) {
	key := "aws:iam:account:id:" + accountID
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		account, ok, e := cache.Get(context.Background(), key)
		if e == nil && ok {
			obj, yes := account.(*meta.IAMAccount)
			if !yes {
				logger.GetLogger("boulder").Errorf("Cached account %s is not of type *meta.IAMAccount", accountID)
				return nil, fmt.Errorf("invalid account type in cache")
			}
			return obj, nil
		}
	}
	var account meta.IAMAccount
	exist, err := s.iam.Get(context.Background(), key, &account)
	if err != nil || !exist {
		logger.GetLogger("boulder").Errorf("failed to get account %s: %v", accountID, err)
		return nil, err
	}

	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		cache.Set(context.Background(), key, &account, time.Second*600)
	}

	return &account, nil
}

// UpdateAccount 更新IAM账户
func (s *IamService) UpdateAccount(accountID string, updateFunc func(*meta.IAMAccount) error) (bool, error) {
	if meta.ValidateAccountID(accountID) != nil {
		logger.GetLogger("boulder").Errorf("invalid account id: %s", accountID)
		return false, errors.New("invalid account id")
	}

	key := "aws:iam:account:id:" + accountID
	var account meta.IAMAccount
	exists, err := s.iam.Get(context.Background(), key, &account)
	if err != nil || !exists {
		logger.GetLogger("boulder").Errorf("failed to get account %s: %v", accountID, err)
		return false, err
	}

	//再记录 access key
	oldAccessKey := account.GetAllAccessKeys()

	// 应用更新
	if err = updateFunc(&account); err != nil {
		logger.GetLogger("boulder").Errorf("failed to update account %s: %v", accountID, err)
		return false, err
	}

	// 更新账户数据
	err = s.iam.Put(context.Background(), key, account)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete account %s: %v", accountID, err)
		return false, err
	}

	//新的 access key 索引
	newAccessKey := account.GetAllAccessKeys()
	del, add := utils.SliceDiff(oldAccessKey, newAccessKey, func(a, b *meta.AccessKey) bool {
		return a.AccessKeyID == b.AccessKeyID
	})

	var changeKeys []string
	for _, accessKey := range del {
		k := "aws:iam:account:ak:" + accessKey.AccessKeyID
		changeKeys = append(changeKeys, k)
		// 删除 access key
		e := s.iam.Delete(context.Background(), k)
		if e != nil {
			logger.GetLogger("boulder").Errorf("failed to delete access key %s: %v", accessKey.AccessKeyID, e)
			return false, e
		}
	}

	for _, accessKey := range add {
		k := "aws:iam:account:ak:" + accessKey.AccessKeyID
		changeKeys = append(changeKeys, k)
		// 添加 access key
		e := s.iam.Put(context.Background(), k, accessKey)
		if e != nil {
			logger.GetLogger("boulder").Errorf("failed to add access key %s: %v", accessKey.AccessKeyID, e)
			return false, e
		}
	}

	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		cache.Del(context.Background(), key)
		cache.BatchDel(context.Background(), changeKeys)
	}

	return true, nil
}

// DeleteAccount 删除IAM账户
func (s *IamService) DeleteAccount(accountID string) error {
	key := "aws:iam:account:id:" + accountID

	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		cache.Del(context.Background(), key)
	}

	// 删除与之相关的所有 accesskey
	var account meta.IAMAccount
	exists, err := s.iam.Get(context.Background(), key, &account)
	if err != nil || !exists {
		logger.GetLogger("boulder").Errorf("failed to get account %s: %v", accountID, err)
		return err
	}

	allAccessKeys := account.GetAllAccessKeys()
	allDel := make([]string, len(allAccessKeys))
	for _, accessKey := range allAccessKeys {
		if accessKey != nil {
			k := "aws:iam:account:ak:" + accessKey.AccessKeyID
			s.iam.Delete(context.Background(), k)
			allDel = append(allDel, k)
		}
	}

	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		cache.BatchDel(context.Background(), allDel)
	}

	// 还要删除与之相关的所有  block, chunk，bucket，和 object 元数据索引 todo
	return s.iam.Delete(context.Background(), key)
}

// CreateUser 为指定账户添加新用户
func (s *IamService) CreateUser(accountID, username, password, path string) (*meta.IAMUser, error) {
	// 验证用户名和密码
	if err := meta.ValidateUsername(username); err != nil {
		logger.GetLogger("boulder").Errorf("username %s is invalid format", username)
		return nil, errors.New("invalid username format")
	}
	if err := meta.ValidatePassword(password, username); err != nil {
		logger.GetLogger("boulder").Errorf("password for user %s is invalid: %v", username, err)
		return nil, fmt.Errorf("invalid password: %w", err)
	}

	// 验证路径格式
	if !meta.ValidatePath(path) {
		logger.GetLogger("boulder").Errorf("invalid path format: %s", path)
		return nil, errors.New("invalid path format")
	}

	var user *meta.IAMUser
	ok, err := s.UpdateAccount(accountID, func(a *meta.IAMAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}

		// 检查用户是否已存在
		if _, exists := a.Users[username]; exists {
			logger.GetLogger("boulder").Errorf("user %s already exists in account %s", username, accountID)
			return fmt.Errorf("user already exists in account %s", accountID)
		}

		// 创建新用户
		var err error
		user, err = a.CreateUser(username, password, path)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to create user %s in account %s: %v", username, accountID, err)
			return err
		}

		a.Users[username] = user
		return nil
	})

	if err != nil || !ok {
		return nil, fmt.Errorf("failed to update account %s: %v", accountID, err)
	}
	return user, nil
}

func (s *IamService) DeleteUser(accountID, username string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IAMAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		return a.DeleteUser(username)
	})

	if err != nil || !ok {
		return err
	}
	return nil
}

func (s *IamService) CreateAccessKey(accountID, username string, expiredAt time.Time) (*meta.AccessKey, error) {
	var key *meta.AccessKey
	ok, err := s.UpdateAccount(accountID, func(a *meta.IAMAccount) error {
		if a == nil {
			logger.GetLogger("boulder").Errorf("account %s not found", accountID)
			return fmt.Errorf("account %s not found", accountID)
		}
		var err error
		key, err = a.CreateAccessKey(username, expiredAt)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to create access key %s in account %s: %v", username, accountID, err)
		}
		return err
	})

	if err != nil || !ok {
		return nil, err
	}
	return key, nil
}

func (s *IamService) DeleteAccessKey(accountID, accessKeyID string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IAMAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		for _, user := range a.Users {
			keys := make([]meta.AccessKey, 0, len(user.AccessKeys)-1)
			for _, ak := range user.AccessKeys {
				if ak.AccessKeyID != accessKeyID {
					keys = append(keys, ak)
				}
			}
			user.AccessKeys = keys
		}
		return nil
	})

	if err != nil || !ok {
		return err
	}
	return nil
}

func (s *IamService) GetAccessKey(accessKeyID string) (*meta.AccessKey, error) {
	if accessKeyID == "" {
		return nil, errors.New("access key id is empty")
	}

	key := "aws:iam:account:ak:" + accessKeyID
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		ak, ok, e := cache.Get(context.Background(), key)
		if e == nil && ok {
			obj, yes := ak.(*meta.AccessKey)
			if !yes {
				logger.GetLogger("boulder").Errorf("invalid type in cache for access key %s ak %T", accessKeyID, ak)
				return nil, fmt.Errorf("invalid access key type in cache")
			}
			return obj, nil
		}
	}

	ak := meta.AccessKey{}
	if ok, err := s.iam.Get(context.Background(), key, &ak); err != nil || !ok {
		logger.GetLogger("boulder").Errorf("get accesskey id %s failed", accessKeyID)
		return nil, fmt.Errorf("access key %s not found", accessKeyID)
	}

	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		cache.Set(context.Background(), key, &ak, time.Second*600)
	}
	return &ak, nil
}
