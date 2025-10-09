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
package iam

import (
	"context"
	"errors"
	"fmt"
	xhttp "github.com/mageg-x/dedups3/internal/http"

	"sync"
	"time"

	"github.com/mageg-x/dedups3/internal/logger"
	xcache "github.com/mageg-x/dedups3/internal/storage/cache"
	"github.com/mageg-x/dedups3/internal/storage/kv"
	"github.com/mageg-x/dedups3/internal/utils"
	"github.com/mageg-x/dedups3/meta"
)

var (
	ERR_ACCOUNT_EXISTS    = errors.New("account already exists")
	ERR_ACCOUNT_NOTEXISTS = errors.New("account not exists")
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
			logger.GetLogger("dedups3").Errorf("failed to get kv store: %v", err)
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
func (s *IamService) CreateAccount(username, password string) (*meta.IamAccount, error) {
	if err := meta.ValidateUsername(username); err != nil {
		logger.GetLogger("dedups3").Errorf("username %s is invalid format", username)
		return nil, errors.New("invalid username format")
	}
	if err := meta.ValidatePassword(password, username); err != nil {
		logger.GetLogger("dedups3").Errorf("password for user %s is invalid: %v", username, err)
		return nil, fmt.Errorf("invalid password: %w", err)
	}

	accountID := meta.GenerateAccountID(username)
	key := "aws:iam:account:id:" + accountID

	txn, err := s.iam.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}

	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 检查账户是否已存在
	var ac meta.IamAccount
	exists, err := txn.Get(key, &ac)
	if exists {
		logger.GetLogger("dedups3").Infof("username %s already exists", username)
		return &ac, ERR_ACCOUNT_EXISTS
	}

	// 创建新账户
	account := meta.CreateAccount(username)

	// 创建根用户
	rootUser, err := account.CreateRootUser(username, password)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to create root user for account %s: %v", account.AccountID, err)
		return nil, err
	}

	if err = txn.Set(key, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to store account %s data in transaction: %v", account.AccountID, err)
		return nil, err
	}

	// 更新 access key 索引
	key = "aws:iam:account:ak:" + rootUser.AccessKeys[0].AccessKeyID
	if err = txn.Set(key, &rootUser.AccessKeys[0]); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to store account %s access key in transaction: %v", account.AccountID, err)
		return nil, err
	}
	if err = txn.Commit(); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, err
	}
	txn = nil
	logger.GetLogger("dedups3").Infof("account %s created with user %s", account.AccountID, username)
	return account, nil
}

// GetAccount 获取IAM账户
func (s *IamService) GetAccount(accountID string) (*meta.IamAccount, error) {
	key := "aws:iam:account:id:" + accountID
	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		account, ok, e := xcache.Get[meta.IamAccount](cache, context.Background(), key)
		if e == nil && ok {
			return account, nil
		} else {
			cache.Del(context.Background(), key)
		}
	}
	var account meta.IamAccount
	exist, err := s.iam.Get(key, &account)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account %s: %v", accountID, err)
		return nil, fmt.Errorf("failed to get account %s: %w", accountID, err)
	}
	if !exist {
		logger.GetLogger("dedups3").Errorf("account %s does not exist", accountID)
		return nil, ERR_ACCOUNT_NOTEXISTS
	}

	if cache, e := xcache.GetCache(); e == nil && cache != nil {
		cache.Set(context.Background(), key, &account, time.Second*600)
	}

	return &account, nil
}

// UpdateAccount 更新IAM账户
func (s *IamService) UpdateAccount(accountID string, updateFunc func(*meta.IamAccount) error) (bool, error) {
	if meta.ValidateAccountID(accountID) != nil {
		logger.GetLogger("dedups3").Errorf("invalid account id: %s", accountID)
		return false, errors.New("invalid account id")
	}

	txn, err := s.iam.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return false, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	key := "aws:iam:account:id:" + accountID
	var account meta.IamAccount
	exists, err := txn.Get(key, &account)
	if err != nil || !exists {
		logger.GetLogger("dedups3").Errorf("failed to get account %s: %v", accountID, err)
		return false, err
	}

	//再记录 access key
	oldAccessKey := account.GetAllAccessKeys()

	// 应用更新
	if err = updateFunc(&account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to update account %s: %v", accountID, err)
		return false, err
	}

	// 更新账户数据
	err = txn.Set(key, account)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete account %s: %v", accountID, err)
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
		e := txn.Delete(k)
		if e != nil {
			logger.GetLogger("dedups3").Errorf("failed to delete access key %s: %v", accessKey.AccessKeyID, e)
			return false, e
		}
	}

	for _, accessKey := range add {
		k := "aws:iam:account:ak:" + accessKey.AccessKeyID
		changeKeys = append(changeKeys, k)
		// 添加 access key
		e := txn.Set(k, accessKey)
		if e != nil {
			logger.GetLogger("dedups3").Errorf("failed to add access key %s: %v", accessKey.AccessKeyID, e)
			return false, e
		}
	}
	if err = txn.Commit(); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return false, err
	}
	txn = nil

	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		cache.Del(context.Background(), key)
		cache.MDel(context.Background(), changeKeys)
	}

	return true, nil
}

// DeleteAccount 删除IAM账户
func (s *IamService) DeleteAccount(accountID string) error {
	key := "aws:iam:account:id:" + accountID

	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		cache.Del(context.Background(), key)
	}

	txn, err := s.iam.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// 删除与之相关的所有 accesskey
	var account meta.IamAccount
	exists, err := txn.Get(key, &account)
	if err != nil || !exists {
		logger.GetLogger("dedups3").Errorf("failed to get account %s: %v", accountID, err)
		return err
	}

	allAccessKeys := account.GetAllAccessKeys()
	allDel := make([]string, 0, len(allAccessKeys))
	for _, accessKey := range allAccessKeys {
		if accessKey != nil {
			k := "aws:iam:account:ak:" + accessKey.AccessKeyID
			err = txn.Delete(k)
			if err != nil {
				logger.GetLogger("dedups3").Errorf("failed to delete account %s access key: %v", accountID, err)
				return fmt.Errorf("failed to delete account %s access key: %w", accountID, err)
			}
			allDel = append(allDel, k)
		}
	}

	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		cache.MDel(context.Background(), allDel)
	}

	err = txn.Delete(key)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete account %s: %v", accountID, err)
		return err
	}
	if err = txn.Commit(); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return err
	}
	txn = nil

	// 还要删除与之相关的所有  block, chunk，bucket，和 object 元数据索引 todo
	return nil
}

// CreateUser 为指定账户添加新用户
func (s *IamService) CreateUser(accountID, username, password string, groups, roles, policies []string, enable bool) (*meta.IamUser, error) {
	// 验证用户名和密码
	if err := meta.ValidateUsername(username); err != nil {
		logger.GetLogger("dedups3").Errorf("username %s is invalid format", username)
		return nil, xhttp.ToError(xhttp.ErrInvalidName)
	}

	var user *meta.IamUser
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}

		// 创建新用户
		var err error
		user, err = a.CreateUser(username, password, groups, roles, policies, enable)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to create user %s in account %s: %v", username, accountID, err)
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

// UpdateUser 更新用户
func (s *IamService) UpdateUser(accountID, username, password string, groups, roles, policies []string, enable bool) (*meta.IamUser, error) {
	// 验证用户名和密码
	if err := meta.ValidateUsername(username); err != nil {
		logger.GetLogger("dedups3").Errorf("username %s is invalid format", username)
		return nil, xhttp.ToError(xhttp.ErrInvalidName)
	}

	var user *meta.IamUser
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}

		// 创建新用户
		var err error
		user, err = a.UpdateUser(username, password, groups, roles, policies, enable)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to update user %s in account %s: %v", username, accountID, err)
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
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
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

func (s *IamService) CreateAccessKey(accountID, username string, expiredAt time.Time, ak, sk string) (*meta.AccessKey, error) {
	var key *meta.AccessKey
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			logger.GetLogger("dedups3").Errorf("account %s not found", accountID)
			return fmt.Errorf("account %s not found", accountID)
		}
		var err error
		key, err = a.CreateAccessKey(username, expiredAt, ak, sk)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to create access key %s in account %s: %v", username, accountID, err)
		}
		return err
	})

	if err != nil || !ok {
		return nil, err
	}
	return key, nil
}

func (s *IamService) DeleteAccessKey(accountID, accessKeyID string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
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
		ak, ok, e := xcache.Get[meta.AccessKey](cache, context.Background(), key)
		if e == nil && ok {
			return ak, nil
		} else {
			cache.Del(context.Background(), key)
		}
	}

	ak := meta.AccessKey{}
	if ok, err := s.iam.Get(key, &ak); err != nil || !ok {
		logger.GetLogger("dedups3").Errorf("get accesskey id %s failed", accessKeyID)
		return nil, fmt.Errorf("access key %s not found", accessKeyID)
	}

	if cache, err := xcache.GetCache(); err == nil && cache != nil {
		cache.Set(context.Background(), key, &ak, time.Second*600)
	}
	return &ak, nil
}

func (s *IamService) CreatePolicy(accountID, username, policyname, desc, doc string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		user, err := a.GetUser(username)
		if err != nil || user == nil {
			return xhttp.ToError(xhttp.ErrAdminNoSuchUser)
		}
		_, err = a.CreatePolicy(user, policyname, desc, doc)
		return err
	})

	if err != nil || !ok {
		return err
	}
	return nil
}

func (s *IamService) UpdatePolicy(accountID, username, policyname, desc, doc string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		user, err := a.GetUser(username)
		if err != nil || user == nil {
			return xhttp.ToError(xhttp.ErrAdminNoSuchUser)
		}
		_, err = a.UpdatePolicy(user, policyname, desc, doc)
		return err
	})

	if err != nil || !ok {
		return err
	}
	return nil
}

func (s *IamService) DeletePolicy(accountID, username, policyname string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		user, err := a.GetUser(username)
		if err != nil || user == nil {
			return xhttp.ToError(xhttp.ErrAdminNoSuchUser)
		}
		err = a.DeletePolicy(user, policyname)
		return err
	})

	if err != nil || !ok {
		return err
	}
	return nil
}

func (s *IamService) CreateRole(accountID, username, rolename, desc, assumeRolePolicy string, attachPolicies []string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		user, err := a.GetUser(username)
		if err != nil || user == nil {
			return xhttp.ToError(xhttp.ErrAdminNoSuchUser)
		}
		_, err = a.CreateRole(user, rolename, desc, assumeRolePolicy, attachPolicies)
		return err
	})

	if err != nil || !ok {
		return err
	}
	return nil
}

func (s *IamService) UpdateRole(accountID, username, rolename, desc, assumeRolePolicy string, attachPolicies []string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		user, err := a.GetUser(username)
		if err != nil || user == nil {
			return xhttp.ToError(xhttp.ErrAdminNoSuchUser)
		}
		_, err = a.UpdateRole(user, rolename, desc, assumeRolePolicy, attachPolicies)
		return err
	})

	if err != nil || !ok {
		return err
	}
	return nil
}

func (s *IamService) DeleteRole(accountID, username, rolename string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		user, err := a.GetUser(username)
		if err != nil || user == nil {
			return xhttp.ToError(xhttp.ErrAdminNoSuchUser)
		}
		err = a.DeleteRole(user, rolename)
		return err
	})

	if err != nil || !ok {
		return err
	}
	return nil
}

func (s *IamService) CreateGroup(accountID, username, groupname, desc string, users, policies []string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		user, err := a.GetUser(username)
		if err != nil || user == nil {
			return xhttp.ToError(xhttp.ErrNoSuchGroup)
		}
		_, err = a.CreateGroup(user, groupname, desc, users, policies)
		return err
	})

	if err != nil || !ok {
		return err
	}
	return nil
}

func (s *IamService) UpdateGroup(accountID, username, groupname, desc string, users, policies []string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		user, err := a.GetUser(username)
		if err != nil || user == nil {
			return xhttp.ToError(xhttp.ErrNoSuchGroup)
		}
		_, err = a.UpdateGroup(user, groupname, desc, users, policies)
		return err
	})

	if err != nil || !ok {
		return err
	}
	return nil
}

func (s *IamService) DeleteGroup(accountID, username, groupname string) error {
	ok, err := s.UpdateAccount(accountID, func(a *meta.IamAccount) error {
		if a == nil {
			return fmt.Errorf("account %s not found", accountID)
		}
		user, err := a.GetUser(username)
		if err != nil || user == nil {
			return xhttp.ToError(xhttp.ErrAdminNoSuchUser)
		}
		err = a.DeleteGroup(user, groupname)
		return err
	})

	if err != nil || !ok {
		return err
	}
	return nil
}
