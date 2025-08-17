package iam

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
)

var (
	instance *IAMService = nil
	mu                   = sync.Mutex{}
)

// IAMService 提供IAM账户管理功能
type IAMService struct {
	mutex sync.RWMutex
	iam   kv.KVStore
}

func GetIAMService() *IAMService {
	mu.Lock()
	defer mu.Unlock()
	if instance == nil || instance.iam == nil {
		instance = &IAMService{
			iam:   kv.GetKvStore(),
			mutex: sync.RWMutex{},
		}
	}

	if instance == nil || instance.iam == nil {
		return nil
	}
	return instance
}

// CreateAccount 创建新的IAM账户
// 根据AWS标准，账户ID由系统自动生成，用户提供用户名和密码
func (s *IAMService) CreateAccount(username, password string) (*meta.IAMAccount, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if err := meta.ValidateUsername(username); err != nil {
		logger.GetLogger("boulder").Errorf("username %s is invalid format", username)
		return nil, errors.New("invalid username format")
	}
	if err := meta.ValidatePassword(password, username); err != nil {
		logger.GetLogger("boulder").Errorf("password for user %s is invalid: %v", username, err)
		return nil, fmt.Errorf("invalid password: %w", err)
	}

	key := "aws:iam:account:root:" + username

	// 检查账户是否已存在
	_, exists, err := s.iam.GetRaw(context.Background(), key)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to check username existence: %v", err)
		return nil, err
	}
	if exists {
		logger.GetLogger("boulder").Errorf("username %s already exists", username)
		return nil, errors.New("account already exists")
	}

	// 创建新账户
	account := meta.CreateAccount()

	// 创建根用户
	rootUser, err := account.CreateRootUser(username, password)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to create root user for account %s: %v", account.AccountID, err)
		return nil, err
	}

	// 开始事务
	success := false
	txn, err := s.iam.BeginTxn(context.Background())
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction for account %s: %v", account.AccountID, err)
		return nil, err
	}
	defer func(txn kv.Txn, ctx context.Context) {
		if !success {
			_ = txn.Rollback(ctx)
		}
	}(txn, context.Background())

	// 在事务中执行所有操作
	key = "aws:iam:account:id:" + account.AccountID
	if err := txn.Put(key, account); err != nil {
		logger.GetLogger("boulder").Errorf("failed to store account %s data in transaction: %v", account.AccountID, err)
		return nil, err
	}

	// 更新 name 和 access key 索引
	key = "aws:iam:account:root:" + username
	if err := txn.Put(key, account.AccountID); err != nil {
		logger.GetLogger("boulder").Errorf("failed to store account %s name in transaction: %v", account.AccountID, err)
		return nil, err
	}

	// 更新 access key 索引
	key = "aws:iam:account:ak:" + rootUser.AccessKeys[0].AccessKeyID
	if err := txn.Put(key, &rootUser.AccessKeys[0]); err != nil {
		logger.GetLogger("boulder").Errorf("failed to store account %s access key in transaction: %v", account.AccountID, err)
		return nil, err
	}

	// 提交事务
	if err := txn.Commit(context.Background()); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction for account %s: %v", account.AccountID, err)
		return nil, err
	} else {
		success = true
	}

	logger.GetLogger("boulder").Infof("account %s created with user %s", account.AccountID, username)
	return account, nil
}

// GetAccount 获取IAM账户
func (s *IAMService) GetAccount(accountID string) (*meta.IAMAccount, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	key := "aws:iam:account:id:" + accountID
	data, exists, err := s.iam.GetRaw(context.Background(), key)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get account %s: %v", accountID, err)
		return nil, err
	}
	if !exists {
		logger.GetLogger("boulder").Errorf("account %s not found", accountID)
		return nil, errors.New("account not found")
	}

	var account meta.IAMAccount
	if err := json.Unmarshal(data, &account); err != nil {
		logger.GetLogger("boulder").Errorf("failed to unmarshal account %s data: %v", accountID, err)
		return nil, err
	}

	return &account, nil
}

// UpdateAccount 更新IAM账户
func (s *IAMService) UpdateAccount(accountID string, updateFunc func(*meta.IAMAccount) error) (bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if meta.ValidateAccountID(accountID) != nil {
		logger.GetLogger("boulder").Errorf("invalid account id: %s", accountID)
		return false, errors.New("invalid account id")
	}

	key := "aws:iam:account:id:" + accountID
	var account meta.IAMAccount
	exists, err := s.iam.Get(context.Background(), key, &account)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get account %s: %v", accountID, err)
		return false, err
	}
	if !exists {
		logger.GetLogger("boulder").Errorf("account %s not found", accountID)
		return false, errors.New("account not found")
	}

	//再记录 access key
	oldAccessKey := account.GetAllAccessKeys()

	// 开始事务
	success := false
	txn, err := s.iam.BeginTxn(context.Background())
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction for account %s: %v", account.AccountID, err)
		return false, err
	}
	defer func(txn kv.Txn, ctx context.Context) {
		if !success {
			_ = txn.Rollback(ctx)
		}
	}(txn, context.Background())

	// 应用更新
	if err := updateFunc(&account); err != nil {
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
	for _, accessKey := range del {
		key := "aws:iam:account:ak:" + accessKey.AccessKeyID
		// 删除 access key
		err := s.iam.Delete(context.Background(), key)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to delete access key %s: %v", accessKey.AccessKeyID, err)
			return false, err
		}
	}

	for _, accessKey := range add {
		key := "aws:iam:account:ak:" + accessKey.AccessKeyID
		// 添加 access key
		err := s.iam.Put(context.Background(), key, accessKey)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to add access key %s: %v", accessKey.AccessKeyID, err)
			return false, err
		}
	}

	// 提交事务
	if err := txn.Commit(context.Background()); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit transaction for account %s: %v", account.AccountID, err)
		return false, err
	} else {
		success = true
	}

	return true, nil
}

// DeleteAccount 删除IAM账户
func (s *IAMService) DeleteAccount(accountID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	key := "aws:iam:account:id:" + accountID
	return s.iam.Delete(context.Background(), key)
}

// CreateUser 为指定账户添加新用户
func (s *IAMService) CreateUser(accountID, username, password, path string) (*meta.IAMUser, error) {
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

func (s *IAMService) DeleteUser(accountID, username string) error {
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

func (s *IAMService) CreateAccessKey(accountID, username string, expiredAt time.Time) (*meta.AccessKey, error) {
	var key *meta.AccessKey
	ok, err := s.UpdateAccount(accountID, func(a *meta.IAMAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		var err error
		key, err = a.CreateAccessKey(username, expiredAt)
		return err
	})

	if err != nil || !ok {
		return nil, err
	}
	return key, nil
}

func (s *IAMService) DeleteAccessKey(accountID, accessKeyID string) error {
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

func (s *IAMService) GetAccessKey(accessKeyID string) (*meta.AccessKey, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if accessKeyID == "" {
		return nil, errors.New("access key id is empty")
	}

	key := "aws:iam:account:ak:" + accessKeyID
	ak := meta.AccessKey{}
	if ok, err := s.iam.Get(context.Background(), key, &ak); err != nil || !ok {
		logger.GetLogger("boulder").Errorf("get accesskey id %s failed", accessKeyID)
		return nil, fmt.Errorf("access key %s not found", accessKeyID)
	}
	return &ak, nil
}
