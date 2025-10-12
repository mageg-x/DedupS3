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
	"encoding/json"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"sync"
	"time"

	xconf "github.com/mageg-x/dedups3/internal/config"
	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/meta"
	pconf "github.com/mageg-x/dedups3/plugs/config"
)

const (
	ACCOUNT_PREFIX   = "dedups3:default:iam-account:"
	USER_PREFIX      = "dedups3:default:iam-user:"
	GROUP_PREFIX     = "dedups3:default:iam-group:"
	ROLE_PREFIX      = "dedups3:default:iam-role:"
	POLICY_PREFIX    = "dedups3:default:iam-policy:"
	ACCESSKEY_PREFIX = "dedups3:default:accesskey:"
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
	conf pconf.KVConfigClient
}

func GetIamService() *IamService {
	mu.Lock()
	defer mu.Unlock()

	if instance == nil || instance.conf == nil {
		cfg := xconf.Get()
		c, err := pconf.NewKVConfig(&pconf.Args{
			Driver:    cfg.Database.Driver,
			DSN:       cfg.Database.DSN,
			AuthToken: cfg.Database.AuthToken,
		})
		if err != nil || c == nil {
			logger.GetLogger("dedups3").Errorf("failed to get kv config : %v", err)
			return nil
		}
		instance = &IamService{
			conf: c,
		}
	}

	if instance == nil || instance.conf == nil {
		return nil
	}
	return instance
}

// CreateAccount 创建新的IAM账户
// 根据AWS标准，账户ID由系统自动生成，用户提供用户名和密码
func (s *IamService) CreateAccount(username, password string) (*meta.IamAccount, error) {
	if err := meta.ValidateUsername(username); err != nil {
		logger.GetLogger("dedups3").Errorf("username %s is invalid format", username)
		return nil, xhttp.ToError(xhttp.ErrInvalidName)
	}
	if err := meta.ValidatePassword(password, username); err != nil {
		logger.GetLogger("dedups3").Errorf("password for user %s is invalid: %v", username, err)
		return nil, xhttp.ToError(xhttp.ErrInvalidRequest)
	}

	accountID := meta.GenerateAccountID(username)
	txn, err := s.conf.TxnBegin()
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to initialize  kvconfig txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvconfig txn: %w", err)
	}

	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	// 检查账户是否已存在
	var _ac meta.IamAccount
	accountKey := ACCOUNT_PREFIX + accountID
	ac, err := s.conf.TxnGetKv(txn, accountKey, _ac)

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		logger.GetLogger("dedups3").Errorf("failed to get kv config: %v", err)
		return nil, fmt.Errorf("failed to get kv config: %w", err)
	}

	if ac != nil {
		logger.GetLogger("dedups3").Errorf("username %s already exists", username)
		return nil, ERR_ACCOUNT_EXISTS
	}

	// 创建新账户
	account := meta.CreateAccount(username)

	// 添加根策略 和缺省策略
	rootPolicyName := "root-policy"
	policyMap := map[string]string{
		rootPolicyName:      meta.RootPolicy(),
		"FullS3Policy":      meta.FullS3Policy(),
		"FullIamPolicy":     meta.FullIAMPolicy(),
		"FullConsolePolicy": meta.FullAdminPolicy(),
	}
	for policyName, policyDoc := range policyMap {
		policy := meta.IamPolicy{
			ARN:         meta.FormatPolicyARN(account.AccountID, policyName),
			Name:        policyName,
			Description: policyName,
			Document:    policyDoc,
			CreateAt:    time.Now().UTC(),
		}
		policyKey := POLICY_PREFIX + account.AccountID + ":" + policyName
		if err := s.conf.TxnSetKv(txn, policyKey, &policy); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to set policy to kv config: %v", err)
			return nil, fmt.Errorf("failed to set policy to kv config: %w", err)
		}
		account.Policies[policyName] = struct{}{}
	}

	// 创建根用户
	rootUser, err := account.CreateUser(username, password, nil, nil, []string{rootPolicyName}, true)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to create root user for account %s: %v", account.AccountID, err)
		return nil, err
	}
	rootUser.IsRoot = true
	userKey := USER_PREFIX + account.AccountID + ":" + username
	if err := s.conf.TxnSetKv(txn, userKey, rootUser); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set root user to kv config: %v", err)
		return nil, fmt.Errorf("failed to set root user to kv config: %w", err)
	}
	account.Users[username] = struct{}{}

	// 保存 account
	if err := s.conf.TxnSetKv(txn, accountKey, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set account to kv config: %v", err)
		return nil, fmt.Errorf("failed to set account to kv config: %w", err)
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, err
	}
	txn = ""

	logger.GetLogger("dedups3").Infof("account %s created with user %s", account.AccountID, username)
	return account, nil
}

// GetAccount 获取IAM账户
func (s *IamService) GetAccount(accountID string) (*meta.IamAccount, error) {
	accountKey := ACCOUNT_PREFIX + accountID

	var _ac meta.IamAccount
	ac, err := s.conf.Get(accountKey, _ac)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		logger.GetLogger("dedups3").Errorf("account %s does not exist", accountID)
		return nil, ERR_ACCOUNT_NOTEXISTS
	}
	if err != nil || ac == nil {
		logger.GetLogger("dedups3").Errorf("failed to get account %s: %v", accountID, err)
		return nil, fmt.Errorf("failed to get account %s: %w", accountID, err)
	}

	account, ok := ac.(*meta.IamAccount)
	if !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account %s: %v", accountID, err)
		return nil, fmt.Errorf("failed to get account %s: %w", accountID, err)
	}
	return account, nil
}

// CreateUser 为指定账户添加新用户
func (s *IamService) CreateUser(accountID, username, targetusername, password string, groups, roles, policies []string, enable bool) (*meta.IamUser, error) {
	// 验证用户名和密码
	if err := meta.ValidateUsername(targetusername); err != nil {
		logger.GetLogger("dedups3").Errorf("username %s is invalid format", targetusername)
		return nil, xhttp.ToError(xhttp.ErrInvalidName)
	}

	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.Get(accountKey, _ac)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		logger.GetLogger("dedups3").Errorf("account %s does not exist", accountID)
		return nil, ERR_ACCOUNT_NOTEXISTS
	}
	if err != nil || ac == nil {
		logger.GetLogger("dedups3").Errorf("failed to get account %s: %v", accountID, err)
		return nil, fmt.Errorf("failed to get account %s: %w", accountID, err)
	}

	account, ok := ac.(*meta.IamAccount)
	if !ok {
		logger.GetLogger("dedups3").Errorf("failed to unmarshal account %s: %v", accountID, err)
		return nil, fmt.Errorf("failed to unmarshal account %s: %w", accountID, err)
	}

	user, err := account.CreateUser(targetusername, password, groups, roles, policies, enable)
	if err != nil || user == nil {
		logger.GetLogger("dedups3").Errorf("failed to create user %s in account %s: %v", targetusername, accountID, err)
		return nil, fmt.Errorf("create usser failed %w", err)
	}
	userKey := USER_PREFIX + accountID + ":" + targetusername
	if err = s.conf.TxnSetKv(txn, userKey, user); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set user to kv config: %v", err)
		return nil, fmt.Errorf("failed to set user to kv config: %w", err)
	}

	account.Users[targetusername] = struct{}{}
	if err := s.conf.TxnSetKv(txn, accountKey, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set account to kv config: %v", err)
		return nil, fmt.Errorf("failed to set account to kv config: %w", err)
	}

	// 往相关群组中添加
	for _, gname := range groups {
		groupKey := GROUP_PREFIX + accountID + ":" + gname
		var _g meta.IamGroup
		g, err := s.conf.Get(groupKey, _g)
		if err != nil || g == nil {
			logger.GetLogger("dedups3").Errorf("failed to get group %s: %v", gname, err)
			return nil, fmt.Errorf("failed to get group %s: %w", gname, err)
		}
		group, ok := g.(*meta.IamGroup)
		if !ok || group == nil {
			logger.GetLogger("dedups3").Errorf("failed to get group %s: %v", gname, err)
			return nil, fmt.Errorf("failed to get group %s: %w", gname, err)
		}
		group.Users = make(meta.StringSet)
		group.Users[targetusername] = struct{}{}
		if err := s.conf.TxnSetKv(txn, groupKey, group); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to set group to kv config: %v", err)
			return nil, fmt.Errorf("failed to set group to kv config: %w", err)
		}
	}

	if err = s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""
	return user, nil
}

// GetUser 根据用户名获取用户信息，包括root用户
func (s *IamService) GetUser(accountID, username, targetusername string) (*meta.IamUser, error) {
	userKey := USER_PREFIX + accountID + ":" + targetusername
	var _u meta.IamUser
	u, err := s.conf.Get(userKey, _u)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		logger.GetLogger("dedups3").Errorf("user %s does not exist", targetusername)
		return nil, xhttp.ToError(xhttp.ErrAdminNoSuchUser)
	}
	if err != nil || u == nil {
		logger.GetLogger("dedups3").Errorf("failed to get user %s: %v", targetusername, err)
		return nil, fmt.Errorf("failed to get user %s: %w", targetusername, err)
	}
	user, ok := u.(*meta.IamUser)
	if !ok || user == nil {
		logger.GetLogger("dedups3").Errorf("failed to get user %s: %v", targetusername, err)
		return nil, fmt.Errorf("failed to get user %s: %w", targetusername, err)
	}
	return user, nil
}

// UpdateUser 更新用户
func (s *IamService) UpdateUser(accountID, username, targetusername, password string, groups, roles, policies []string, enable bool) (*meta.IamUser, error) {
	// 验证用户名和密码
	if err := meta.ValidateUsername(targetusername); err != nil {
		logger.GetLogger("dedups3").Errorf("username %s is invalid format", targetusername)
		return nil, xhttp.ToError(xhttp.ErrInvalidName)
	}

	ac, err := s.GetAccount(accountID)
	if err != nil || ac == nil {
		logger.GetLogger("dedups3").Errorf("failed to get account %s: %v", accountID, err)
		return nil, fmt.Errorf("failed to get account %s: %w", accountID, err)
	}

	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	userKey := USER_PREFIX + accountID + ":" + targetusername
	var _u meta.IamUser
	u, err := s.conf.TxnGetKv(txn, userKey, _u)
	if err != nil || u == nil {
		logger.GetLogger("dedups3").Errorf("failed to get user to kv config: %v", err)
		return nil, fmt.Errorf("failed to get user to kv config: %w", err)
	}
	user, ok := u.(*meta.IamUser)
	if !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account %s: %v", accountID, err)
		return nil, fmt.Errorf("failed to get account %s: %w", accountID, err)
	}

	if password != "" {
		if err := meta.ValidatePassword(password, targetusername); err != nil {
			logger.GetLogger("dedups3").Errorf("password for user %s is invalid: %v", targetusername, err)
			return nil, xhttp.ToError(xhttp.ErrInvalidRequest)
		}
		user.Password = password
	}
	user.Enabled = enable

	user.Groups = make(meta.StringSet)
	for _, group := range groups {
		if _, exists := ac.Groups[group]; exists {
			user.Groups[group] = struct{}{}
		}
	}

	user.Roles = make(meta.StringSet)
	for _, role := range roles {
		if _, exists := ac.Roles[role]; exists {
			user.Roles[role] = struct{}{}
		}
	}

	user.AttachedPolicies = make(meta.StringSet)
	for _, policy := range policies {
		if _, exists := ac.Policies[policy]; exists {
			user.AttachedPolicies[policy] = struct{}{}
		}
	}

	if err := s.conf.TxnSetKv(txn, userKey, user); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set user to kv config: %v", err)
		return nil, fmt.Errorf("failed to set user to kv config: %w", err)
	}

	// 往相关群组中添加
	for _, gname := range groups {
		groupKey := GROUP_PREFIX + accountID + ":" + gname
		var _g meta.IamGroup
		g, err := s.conf.Get(groupKey, _g)
		if err != nil || g == nil {
			logger.GetLogger("dedups3").Errorf("failed to get group %s: %v", gname, err)
			return nil, fmt.Errorf("failed to get group %s: %w", gname, err)
		}
		group, ok := g.(*meta.IamGroup)
		if !ok || group == nil {
			logger.GetLogger("dedups3").Errorf("failed to get group %s: %v", gname, err)
			return nil, fmt.Errorf("failed to get group %s: %w", gname, err)
		}
		group.Users = make(meta.StringSet)
		group.Users[targetusername] = struct{}{}
		if err := s.conf.TxnSetKv(txn, groupKey, group); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to set group to kv config: %v", err)
			return nil, fmt.Errorf("failed to set group to kv config: %w", err)
		}
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""

	return user, nil
}

func (s *IamService) DeleteUser(accountID, username, targetusername string) error {
	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	userKey := USER_PREFIX + accountID + ":" + targetusername
	var _u meta.IamUser
	u, err := s.conf.TxnGetKv(txn, userKey, _u)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return xhttp.ToError(xhttp.ErrAdminNoSuchUser)
	}

	if err != nil || u == nil {
		logger.GetLogger("dedups3").Errorf("failed to get user to kv config: %v", err)
		return fmt.Errorf("failed to get user to kv config: %w", err)
	}
	user, ok := u.(*meta.IamUser)
	if !ok || user == nil {
		logger.GetLogger("dedups3").Errorf("failed to get user to kv config: %v", err)
		return fmt.Errorf("failed to get user to kv config: %w", err)
	}
	if user.IsRoot {
		return errors.New("cannot delete root user")
	}

	// 从group中删除关联user
	for gname := range user.Groups {
		groupKey := GROUP_PREFIX + accountID + ":" + gname
		var _g meta.IamGroup
		g, err := s.conf.TxnGetKv(txn, groupKey, _g)
		if g == nil || err != nil {
			logger.GetLogger("dedups3").Errorf("failed to get group to kv config: %v", err)
			continue
		}
		group, ok := g.(*meta.IamGroup)
		if !ok || group == nil {
			logger.GetLogger("dedups3").Errorf("failed to get group to kv config: %v", err)
			return fmt.Errorf("failed to get group to kv config: %w", err)
		}
		delete(group.Users, targetusername)
		if err := s.conf.TxnSetKv(txn, groupKey, group); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to set group to kv config: %v", err)
			return fmt.Errorf("failed to set group to kv config: %w", err)
		}
	}

	// 删除 user 记录
	if err := s.conf.TxnDelKv(txn, userKey); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete user to kv config: %v", err)
		return fmt.Errorf("failed to delete user to kv config: %w", err)
	}

	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.TxnGetKv(txn, accountKey, _ac)
	if err != nil || ac == nil {
		logger.GetLogger("dedups3").Errorf("failed to get account to kv config: %v", err)
		return fmt.Errorf("failed to get account to kv config: %w", err)
	}
	account, ok := ac.(*meta.IamAccount)
	if !ok || account == nil {
		logger.GetLogger("dedups3").Errorf("failed to get account to kv config: %v", err)
		return fmt.Errorf("failed to get account to kv config: %w", err)
	}
	// 从account 中删除 user
	delete(account.Users, targetusername)
	if err := s.conf.TxnSetKv(txn, accountKey, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set account to kv config: %v", err)
		return fmt.Errorf("failed to set account to kv config: %w", err)
	}

	// 删除user的access key
	for ak := range user.AccessKeys {
		akKey := ACCESSKEY_PREFIX + ak
		if err := s.conf.TxnDelKv(txn, akKey); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to delete account to kv config: %v", err)
			return fmt.Errorf("failed to delete account to kv config: %w", err)
		}
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	txn = ""

	return nil
}

func (s *IamService) CreateAccessKey(accountID, username string, ak, sk string, expiredAt time.Time, enable bool) (*meta.AccessKey, error) {
	if ak != "" {
		if !meta.ValidateAccessKeyID(ak) {
			return nil, xhttp.ToError(xhttp.ErrInvalidAccessKeyID)
		}
		if _ak, _ := s.GetAccessKey(ak); _ak != nil {
			return nil, xhttp.ToError(xhttp.ErrAdminConfigDuplicateKeys)
		}
	}
	if sk != "" && !meta.ValidateSecretAccessKey(sk) {
		return nil, xhttp.ToError(xhttp.ErrAdminInvalidSecretKey)
	}

	if expiredAt.Before(time.Now()) {
		return nil, errors.New("expired time cannot be before current time")
	}
	if ak == "" {
		ak = meta.GenerateAccessKeyID()
	}
	if sk == "" {
		sk = meta.GenerateSecretAccessKey()
	}

	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()
	// 是否已经存在， ak是全局唯一
	akKey := ACCESSKEY_PREFIX + ak
	var _ak meta.AccessKey
	ack, err := s.conf.TxnGetKv(txn, akKey, _ak)
	if ack != nil {
		logger.GetLogger("dedups3").Errorf(" access key %s already exists : %v", ak, err)
		return nil, xhttp.ToError(xhttp.ErrAdminConfigDuplicateKeys)
	}
	accessKey := meta.AccessKey{
		AccessKeyID:     ak,
		SecretAccessKey: sk,
		CreatedAt:       time.Now().UTC(),
		ExpiredAt:       expiredAt,
		Username:        username,
		AccountID:       accountID,
		Status:          enable,
	}

	if err := s.conf.TxnSetKv(txn, akKey, &accessKey); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set access key to kv config: %v", err)
		return nil, fmt.Errorf("failed to set access key to kv config: %w", err)
	}

	userKey := USER_PREFIX + accountID + ":" + username
	var _u meta.IamUser
	u, err := s.conf.TxnGetKv(txn, userKey, _u)
	if err != nil || u == nil {
		logger.GetLogger("dedups3").Errorf("failed to get user to kv config: %v", err)
		return nil, fmt.Errorf("failed to get user to kv config: %w", err)
	}

	user, ok := u.(*meta.IamUser)
	if !ok {
		logger.GetLogger("dedups3").Errorf("failed to unmarshal account %s: %#v", accountID, u)
		return nil, fmt.Errorf("failed to unmarshal account %s", accountID)
	}

	user.AccessKeys[ak] = struct{}{}

	if err := s.conf.TxnSetKv(txn, userKey, user); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set user to kv config: %v", err)
		return nil, fmt.Errorf("failed to set user to kv config: %w", err)
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""

	return &accessKey, nil
}

func (s *IamService) UpdateAccessKey(accountID, ak, sk string, expiredAt time.Time, enable bool) (*meta.AccessKey, error) {
	if ak == "" || !meta.ValidateAccessKeyID(ak) {
		return nil, xhttp.ToError(xhttp.ErrInvalidAccessKeyID)
	}
	if _ak, _ := s.GetAccessKey(ak); _ak == nil {
		return nil, xhttp.ToError(xhttp.ErrAdminNoSuchAccessKey)
	}
	if sk == "" || !meta.ValidateSecretAccessKey(sk) {
		return nil, xhttp.ToError(xhttp.ErrAdminInvalidSecretKey)
	}

	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()
	// 是否已经存在， ak是全局唯一
	akKey := ACCESSKEY_PREFIX + ak
	var _ak meta.AccessKey
	ack, err := s.conf.TxnGetKv(txn, akKey, _ak)
	if ack == nil {
		logger.GetLogger("dedups3").Errorf(" access key %s not exists : %v", ak, err)
		return nil, xhttp.ToError(xhttp.ErrAdminNoSuchAccessKey)
	}

	accessKey, ok := ack.(*meta.AccessKey)
	if !ok {
		logger.GetLogger("dedups3").Errorf("failed to get access key from kv config: %v", err)
		return nil, xhttp.ToError(xhttp.ErrAdminInvalidAccessKey)
	}
	accessKey.Status = enable
	accessKey.ExpiredAt = expiredAt
	accessKey.SecretAccessKey = sk

	if err := s.conf.TxnSetKv(txn, akKey, &accessKey); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set access key to kv config: %v", err)
		return nil, fmt.Errorf("failed to set access key to kv config: %w", err)
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""

	return accessKey, nil
}

func (s *IamService) DeleteAccessKey(accountID, ak string) error {
	if ak == "" || !meta.ValidateAccessKeyID(ak) {
		return xhttp.ToError(xhttp.ErrInvalidAccessKeyID)
	}
	if _ak, _ := s.GetAccessKey(ak); _ak == nil {
		return xhttp.ToError(xhttp.ErrAdminNoSuchAccessKey)
	}
	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()
	// 是否已经存在， ak是全局唯一
	akKey := ACCESSKEY_PREFIX + ak

	if err := s.conf.TxnDelKv(txn, akKey); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete access key to kv config: %v", err)
		return fmt.Errorf("failed to delete access key to kv config: %w", err)
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""

	return nil
}

func (s *IamService) GetAccessKey(accessKeyID string) (*meta.AccessKey, error) {
	if accessKeyID == "" {
		return nil, errors.New("access key id is empty")
	}
	var _ak meta.AccessKey
	akKey := ACCESSKEY_PREFIX + accessKeyID
	ak, err := s.conf.Get(akKey, _ak)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get access key from kv config: %v", err)
		return nil, fmt.Errorf("failed to get access key from kv config: %w", err)
	}

	accessKey, ok := ak.(*meta.AccessKey)
	if !ok {
		logger.GetLogger("dedups3").Errorf("failed to get access key from kv config: %v", err)
		return nil, fmt.Errorf("failed to get access key from kv config: %w", err)
	}

	return accessKey, nil
}

func (s *IamService) CreatePolicy(accountID, username, policyname, desc, doc string) (*meta.IamPolicy, error) {
	if !meta.IsValidIAMName(policyname) {
		return nil, xhttp.ToError(xhttp.ErrInvalidName)
	}
	// 验证策略文档
	if err := meta.ValidatePolicyDocument(doc); err != nil {
		return nil, xhttp.ToError(xhttp.ErrInvalidPolicyDocument)
	}
	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.Get(accountKey, _ac)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		logger.GetLogger("dedups3").Errorf("account %s does not exist", accountID)
		return nil, ERR_ACCOUNT_NOTEXISTS
	}
	if err != nil || ac == nil {
		logger.GetLogger("dedups3").Errorf("failed to get account %s: %v", accountID, err)
		return nil, fmt.Errorf("failed to get account %s: %w", accountID, err)
	}
	account, ok := ac.(*meta.IamAccount)
	if !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account %s: %v", accountID, err)
		return nil, fmt.Errorf("failed to get account %s: %w", accountID, err)
	}

	if _, exists := account.Policies[policyname]; exists {
		logger.GetLogger("dedups3").Errorf("policy %s already exists", policyname)
		return nil, xhttp.ToError(xhttp.ErrPolicyAlreadyExists)
	}

	policy := meta.IamPolicy{
		ARN:         meta.FormatPolicyARN(accountID, policyname),
		Name:        policyname,
		Description: desc,
		Document:    doc,
		CreateAt:    time.Now().UTC(),
	}

	policyKey := POLICY_PREFIX + accountID + ":" + policyname
	if err := s.conf.TxnSetKv(txn, policyKey, &policy); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set policy to kv config: %v", err)
		return nil, fmt.Errorf("failed to set policy to kv config: %w", err)
	}

	account.Policies[policyname] = struct{}{}
	if err := s.conf.TxnSetKv(txn, accountKey, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set policy to kv config: %v", err)
		return nil, fmt.Errorf("failed to set policy to kv config: %w", err)
	}
	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""

	return &policy, nil
}

func (s *IamService) GetPolicy(accountID, username, policyname string) (*meta.IamPolicy, error) {
	policyKey := POLICY_PREFIX + accountID + ":" + policyname
	var _p meta.IamPolicy
	p, err := s.conf.Get(policyKey, _p)
	if p == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get policy %s: %v", policyname, err)
		return nil, fmt.Errorf("failed to get policy %s: %w", policyname, err)
	}
	policy, ok := p.(*meta.IamPolicy)
	if !ok || policy == nil {
		logger.GetLogger("dedups3").Errorf("failed to get policy %s: %v", policyname, err)
		return nil, fmt.Errorf("failed to get policy %s: %w", policyname, err)
	}
	return policy, nil
}
func (s *IamService) UpdatePolicy(accountID, username, policyname, desc, doc string) (*meta.IamPolicy, error) {
	if !meta.IsValidIAMName(policyname) {
		return nil, xhttp.ToError(xhttp.ErrInvalidName)
	}
	// 验证策略文档
	if err := meta.ValidatePolicyDocument(doc); err != nil {
		return nil, xhttp.ToError(xhttp.ErrInvalidPolicyDocument)
	}
	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()
	policyKey := POLICY_PREFIX + accountID + ":" + policyname
	var _policy meta.IamPolicy
	p, err := s.conf.TxnGetKv(txn, policyKey, _policy)
	if p == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get policy from kv config: %v", err)
		return nil, fmt.Errorf("failed to get policy from kv config: %w", err)
	}
	policy, ok := p.(*meta.IamPolicy)
	if !ok {
		logger.GetLogger("dedups3").Errorf("failed to get policy from kv config: %v", err)
		return nil, fmt.Errorf("failed to get policy from kv config: %w", err)
	}
	policy.Description = desc
	policy.Document = doc
	if err := s.conf.TxnSetKv(txn, policyKey, policy); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set policy to kv config: %v", err)
		return nil, fmt.Errorf("failed to set policy to kv config: %w", err)
	}
	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""

	return policy, nil
}

func (s *IamService) DeletePolicy(accountID, username, policyname string) error {
	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	// 从 account 中删除策略
	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.TxnGetKv(txn, accountKey, _ac)
	if ac == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return fmt.Errorf("failed to get account from kv config: %w", err)
	}
	account, ok := ac.(*meta.IamAccount)
	if account == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return fmt.Errorf("failed to get account from kv config: %w", err)
	}
	delete(account.Policies, policyname)
	if err := s.conf.TxnSetKv(txn, accountKey, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set policy to kv config: %v", err)
		return fmt.Errorf("failed to set policy to kv config: %w", err)
	}

	// 从所有用户中移除该策略
	for uname := range account.Users {
		userKey := USER_PREFIX + accountID + ":" + uname
		var _u meta.IamUser
		_user, err := s.conf.TxnGetKv(txn, userKey, _u)
		if _user == nil || err != nil {
			logger.GetLogger("dedups3").Errorf("failed to get user from kv config: %v", err)
			continue
		}
		user, ok := _user.(*meta.IamUser)
		if !ok || user == nil {
			logger.GetLogger("dedups3").Errorf("failed to get user from kv config: %v", err)
			return fmt.Errorf("failed to get user from kv config: %w", err)
		}
		delkeys := make([]string, 0)
		for pname := range user.AttachedPolicies {
			if _, ok := account.Policies[pname]; !ok {
				delkeys = append(delkeys, pname)
			}
		}
		for _, key := range delkeys {
			delete(user.AttachedPolicies, key)
		}
		if len(delkeys) > 0 {
			if err := s.conf.TxnSetKv(txn, userKey, user); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to set policy to kv config: %v", err)
				return fmt.Errorf("failed to set user to kv config: %w", err)
			}
		}
	}

	// 从所有组中移除该策略
	for gname := range account.Groups {
		groupKey := GROUP_PREFIX + accountID + ":" + gname
		var _g meta.IamGroup
		g, err := s.conf.TxnGetKv(txn, groupKey, _g)
		if g == nil || err != nil {
			logger.GetLogger("dedups3").Errorf("failed to get group from kv config: %v", err)
			continue
		}
		group, ok := g.(*meta.IamGroup)
		if !ok || group == nil {
			logger.GetLogger("dedups3").Errorf("failed to get group from kv config: %v", err)
			return fmt.Errorf("failed to get group from kv config: %w", err)
		}

		delkeys := make([]string, 0)
		for pname := range group.AttachedPolicies {
			if _, ok := account.Policies[pname]; !ok {
				delkeys = append(delkeys, pname)
			}
		}
		for _, key := range delkeys {
			delete(group.AttachedPolicies, key)
		}
		if len(delkeys) > 0 {
			if err := s.conf.TxnSetKv(txn, groupKey, group); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to set policy to kv config: %v", err)
				return fmt.Errorf("failed to set group to kv config: %w", err)
			}
		}
	}

	// 从所有角色中移除该策略
	for rname := range account.Roles {
		roleKey := ROLE_PREFIX + accountID + ":" + rname
		var _r meta.IamRole
		r, err := s.conf.TxnGetKv(txn, roleKey, _r)
		if r == nil || err != nil {
			logger.GetLogger("dedups3").Errorf("failed to get role from kv config: %v", err)
			continue
		}
		role, ok := r.(*meta.IamRole)
		if !ok || role == nil {
			logger.GetLogger("dedups3").Errorf("failed to get role from kv config: %v", err)
			return fmt.Errorf("failed to get role from kv config: %w", err)
		}

		delkeys := make([]string, 0)
		for pname := range role.AttachedPolicies {
			if _, ok := account.Policies[pname]; !ok {
				delkeys = append(delkeys, pname)
			}
		}

		if len(delkeys) > 0 {
			if err := s.conf.TxnSetKv(txn, roleKey, role); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to set policy to kv config: %v", err)
				return fmt.Errorf("failed to set role to kv config: %w", err)
			}
		}

	}

	// 从策略集中删除策略
	policyKey := POLICY_PREFIX + accountID + ":" + policyname
	if err := s.conf.TxnDelKv(txn, policyKey); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete policy from kv config: %v", err)
		return fmt.Errorf("failed to delete policy from kv config: %w", err)
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""
	return nil
}

func (s *IamService) CreateRole(accountID, username, rolename, desc, assumeRolePolicy string, attachPolicies []string) (*meta.IamRole, error) {
	if !meta.IsValidIAMName(rolename) {
		return nil, xhttp.ToError(xhttp.ErrInvalidName)
	}

	// 验证信任策略是否是有效的 JSON
	if assumeRolePolicy != "" && !meta.IsValidJSON(assumeRolePolicy) {
		return nil, xhttp.ToError(xhttp.ErrInvalidPolicyDocument)
	}

	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.TxnGetKv(txn, accountKey, _ac)
	if ac == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return nil, fmt.Errorf("failed to get account from kv config: %w", err)
	}
	account, ok := ac.(*meta.IamAccount)
	if account == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return nil, fmt.Errorf("failed to get account from kv config: %w", err)
	}
	if _, exists := account.Roles[rolename]; exists {
		logger.GetLogger("dedups3").Errorf("role %s already exists", rolename)
		return nil, xhttp.ToError(xhttp.ErrRoleAlreadyExists)
	}
	role := meta.IamRole{
		ARN:              meta.FormatRoleARN(accountID, rolename),
		Name:             rolename,
		Description:      desc,
		AssumeRolePolicy: assumeRolePolicy,
		CreateAt:         time.Now().UTC(),
		AttachedPolicies: make(meta.StringSet),
	}
	for _, pname := range attachPolicies {
		role.AttachedPolicies[pname] = struct{}{}
	}
	roleKey := ROLE_PREFIX + accountID + ":" + rolename
	if err := s.conf.TxnSetKv(txn, roleKey, &role); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set role to kv config: %v", err)
		return nil, fmt.Errorf("failed to set role to kv config: %w", err)
	}

	account.Roles[rolename] = struct{}{}
	if err := s.conf.TxnSetKv(txn, accountKey, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set role to kv config: %v", err)
		return nil, fmt.Errorf("failed to set role to kv config: %w", err)
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""
	return &role, nil
}

func (s *IamService) GetRole(accountID, username, rolename string) (*meta.IamRole, error) {
	roleKey := ROLE_PREFIX + accountID + ":" + rolename
	var _r meta.IamRole
	r, err := s.conf.Get(roleKey, _r)
	if err != nil || r == nil {
		logger.GetLogger("dedups3").Errorf("failed to get role from kv config: %v", err)
		return nil, fmt.Errorf("failed to get role from kv config: %w", err)
	}
	role, ok := r.(*meta.IamRole)
	if !ok || role == nil {
		logger.GetLogger("dedups3").Errorf("failed to get role from kv config: %v", err)
		return nil, fmt.Errorf("failed to get role from kv config: %w", err)
	}
	return role, nil
}

func (s *IamService) UpdateRole(accountID, username, rolename, desc, assumeRolePolicy string, attachPolicies []string) (*meta.IamRole, error) {
	// 验证信任策略是否是有效的 JSON
	if assumeRolePolicy != "" && !meta.IsValidJSON(assumeRolePolicy) {
		return nil, xhttp.ToError(xhttp.ErrInvalidPolicyDocument)
	}

	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.TxnGetKv(txn, accountKey, _ac)
	if ac == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return nil, fmt.Errorf("failed to get account from kv config: %w", err)
	}
	account, ok := ac.(*meta.IamAccount)
	if account == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return nil, fmt.Errorf("failed to get account from kv config: %w", err)
	}
	if _, exists := account.Roles[rolename]; !exists {
		logger.GetLogger("dedups3").Errorf("role %s not exists", rolename)
		return nil, xhttp.ToError(xhttp.ErrNoSuchRole)
	}

	roleKey := ROLE_PREFIX + accountID + ":" + rolename
	var _r meta.IamRole
	r, err := s.conf.TxnGetKv(txn, roleKey, _r)
	if r == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get role from kv config: %v", err)
		return nil, fmt.Errorf("failed to get role from kv config: %w", err)
	}
	role, ok := r.(*meta.IamRole)
	if role == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get role from kv config: %v", err)
		return nil, fmt.Errorf("failed to get role from kv config: %w", err)
	}
	role.AssumeRolePolicy = assumeRolePolicy
	role.Description = desc
	role.AttachedPolicies = make(meta.StringSet)
	for _, pname := range attachPolicies {
		role.AttachedPolicies[pname] = struct{}{}
	}

	if err := s.conf.TxnSetKv(txn, roleKey, role); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set role to kv config: %v", err)
		return nil, fmt.Errorf("failed to set role to kv config: %w", err)
	}
	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""
	return role, nil
}

func (s *IamService) DeleteRole(accountID, username, rolename string) error {
	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.TxnGetKv(txn, accountKey, _ac)
	if ac == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return fmt.Errorf("failed to get account from kv config: %w", err)
	}
	account, ok := ac.(*meta.IamAccount)
	if account == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return fmt.Errorf("failed to get account from kv config: %w", err)
	}

	delete(account.Roles, rolename)
	if err := s.conf.TxnSetKv(txn, accountKey, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set role to kv config: %v", err)
		return fmt.Errorf("failed to set role to kv config: %w", err)
	}

	roleKey := ROLE_PREFIX + accountID + ":" + rolename
	if err := s.conf.TxnDelKv(txn, roleKey); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete role from kv config: %v", err)
		return fmt.Errorf("failed to delete role from kv config: %w", err)
	}

	// 从所有用户中移除该角色
	for uname := range account.Users {
		userKey := USER_PREFIX + accountID + ":" + uname
		var _u meta.IamUser
		u, err := s.conf.TxnGetKv(txn, userKey, _u)
		if u == nil || err != nil {
			logger.GetLogger("dedups3").Errorf("failed to get user from kv config: %v", err)
			continue
		}
		user, ok := u.(*meta.IamUser)
		if user == nil || !ok {
			logger.GetLogger("dedups3").Errorf("failed to get user from kv config: %v", err)
			return fmt.Errorf("failed to get user from kv config: %w", err)
		}
		delete(user.Roles, rolename)

		delKeys := make([]string, 0, len(user.Roles))
		for rname := range user.Roles {
			if _, exists := account.Roles[rname]; !exists {
				delKeys = append(delKeys, rname)
			}
		}
		if len(delKeys) > 0 {
			for _, key := range delKeys {
				delete(user.Roles, key)
			}
		}
		if err := s.conf.TxnSetKv(txn, userKey, user); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to set role to kv config: %v", err)
			return fmt.Errorf("failed to set role to kv config: %w", err)
		}
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""
	return nil
}

func (s *IamService) CreateGroup(accountID, username, groupname, desc string, users, policies []string) (*meta.IamGroup, error) {
	if !meta.IsValidIAMName(groupname) {
		return nil, xhttp.ToError(xhttp.ErrInvalidName)
	}

	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.TxnGetKv(txn, accountKey, _ac)
	if ac == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return nil, fmt.Errorf("failed to get account from kv config: %w", err)
	}
	account, ok := ac.(*meta.IamAccount)
	if account == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return nil, fmt.Errorf("failed to get account from kv config: %w", err)
	}

	if _, exists := account.Groups[groupname]; exists {
		logger.GetLogger("dedups3").Errorf("group %s already exists", groupname)
		return nil, xhttp.ToError(xhttp.ErrGroupAlreadyExists)
	}
	group := meta.IamGroup{
		ARN:              meta.FormatGroupARN(accountID, groupname),
		Name:             groupname,
		Description:      desc,
		Users:            make(meta.StringSet),
		AttachedPolicies: make(meta.StringSet),
		CreateAt:         time.Now().UTC(),
	}
	for _, uname := range users {
		if _, exists := account.Users[uname]; exists {
			group.Users[uname] = struct{}{}
		}
	}
	for _, pname := range policies {
		if _, exists := account.Policies[pname]; exists {
			group.AttachedPolicies[pname] = struct{}{}
		}
	}
	groupKey := GROUP_PREFIX + accountID + ":" + groupname
	if err := s.conf.TxnSetKv(txn, groupKey, &group); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set group to kv config: %v", err)
		return nil, fmt.Errorf("failed to set group to kv config: %w", err)
	}

	account.Groups[groupname] = struct{}{}
	if err := s.conf.TxnSetKv(txn, accountKey, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set role to kv config: %v", err)
		return nil, fmt.Errorf("failed to set role to kv config: %w", err)
	}
	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""

	return &group, nil
}

func (s *IamService) GetGroup(accountID, username, groupname string) (*meta.IamGroup, error) {
	groupKey := GROUP_PREFIX + accountID + ":" + groupname
	var _g meta.IamGroup
	g, err := s.conf.Get(groupKey, _g)
	if err != nil || g == nil {
		logger.GetLogger("dedups3").Errorf("failed to get group from kv config: %v", err)
		return nil, fmt.Errorf("failed to get group from kv config: %w", err)
	}
	group, ok := g.(*meta.IamGroup)
	if group == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get group from kv config: %v", err)
		return nil, fmt.Errorf("failed to get group from kv config: %w", err)
	}
	return group, nil
}

func (s *IamService) UpdateGroup(accountID, username, groupname, desc string, users, policies []string) (*meta.IamGroup, error) {
	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.TxnGetKv(txn, accountKey, _ac)
	if ac == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return nil, fmt.Errorf("failed to get account from kv config: %w", err)
	}
	account, ok := ac.(*meta.IamAccount)
	if account == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return nil, fmt.Errorf("failed to get account from kv config: %w", err)
	}

	groupKey := GROUP_PREFIX + accountID + ":" + groupname
	var _g meta.IamGroup
	g, err := s.conf.TxnGetKv(txn, groupKey, _g)
	if g == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get group from kv config: %v", err)
		return nil, fmt.Errorf("failed to get group from kv config: %w", err)
	}
	group, ok := g.(*meta.IamGroup)
	if group == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get group from kv config: %v", err)
		return nil, fmt.Errorf("failed to get group from kv config: %w", err)
	}
	group.Description = desc
	group.Users = make(meta.StringSet)
	for _, uname := range users {
		if _, exists := account.Users[uname]; exists {
			group.Users[uname] = struct{}{}
		}
	}

	group.AttachedPolicies = make(meta.StringSet)
	for _, pname := range policies {
		if _, exists := account.Policies[pname]; exists {
			group.AttachedPolicies[pname] = struct{}{}
		}
	}

	if err := s.conf.TxnSetKv(txn, groupKey, group); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set role to kv config: %v", err)
		return nil, fmt.Errorf("failed to set role to kv config: %w", err)
	}
	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""

	return group, nil
}

func (s *IamService) DeleteGroup(accountID, username, groupname string) error {
	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.TxnGetKv(txn, accountKey, _ac)
	if ac == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return fmt.Errorf("failed to get account from kv config: %w", err)
	}
	account, ok := ac.(*meta.IamAccount)
	if account == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return fmt.Errorf("failed to get account from kv config: %w", err)
	}

	delete(account.Groups, groupname)
	if err := s.conf.TxnSetKv(txn, accountKey, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set role to kv config: %v", err)
		return fmt.Errorf("failed to set role to kv config: %w", err)
	}

	// 用户中移除该组
	groupKey := GROUP_PREFIX + accountID + ":" + groupname
	var _g meta.IamGroup
	g, err := s.conf.TxnGetKv(txn, groupKey, _g)
	if g == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get group from kv config: %v", err)
		return fmt.Errorf("failed to get group from kv config: %w", err)
	}
	group, ok := g.(*meta.IamGroup)
	if group == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get group from kv config: %v", err)
		return fmt.Errorf("failed to get group from kv config: %w", err)
	}
	for uname := range group.Users {
		userKey := USER_PREFIX + accountID + ":" + uname
		var _u meta.IamUser
		u, err := s.conf.TxnGetKv(txn, userKey, _u)
		if u == nil || err != nil {
			logger.GetLogger("dedups3").Errorf("failed to get user from kv config: %v", err)
			continue
		}
		user, ok := u.(*meta.IamUser)
		if user == nil || !ok {
			logger.GetLogger("dedups3").Errorf("failed to get user from kv config: %v", err)
			return fmt.Errorf("failed to get user from kv config: %w", err)
		}
		delete(user.Groups, groupname)

		delKeys := make([]string, 0, len(user.Groups))
		for k := range user.Groups {
			if _, exists := account.Groups[k]; !exists {
				delKeys = append(delKeys, k)
			}
		}
		for _, k := range delKeys {
			delete(user.Groups, k)
		}
		if err := s.conf.TxnSetKv(txn, userKey, user); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to set role to kv config: %v", err)
			return fmt.Errorf("failed to set role to kv config: %w", err)
		}

	}
	if err := s.conf.TxnDelKv(txn, groupKey); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set role to kv config: %v", err)
		return fmt.Errorf("failed to set role to kv config: %w", err)
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""
	return nil
}

func (s *IamService) SetQuota(accountID string, quota *meta.QuotaConfig) error {
	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" && s.conf != nil {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	accountKey := ACCOUNT_PREFIX + accountID
	var _ac meta.IamAccount
	ac, err := s.conf.TxnGetKv(txn, accountKey, _ac)
	if ac == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return fmt.Errorf("failed to get account from kv config: %w", err)
	}
	account, ok := ac.(*meta.IamAccount)
	if account == nil || !ok {
		logger.GetLogger("dedups3").Errorf("failed to get account from kv config: %v", err)
		return fmt.Errorf("failed to get account from kv config: %w", err)
	}

	account.Quota = quota

	if err := s.conf.TxnSetKv(txn, accountKey, account); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set quota config: %v", err)
		return fmt.Errorf("failed to set quota config: %w", err)
	}
	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""
	return nil
}

func (s *IamService) DeleteQuota(accountID string) error {
	return s.SetQuota(accountID, nil)
}

// ============================== 权限检查方法 ==============================

// 获取用户所有策略（直接附加+通过组附加+通过角色附加）
func (s *IamService) ListUserAllPolicies(accountID, username string) ([]*meta.IamPolicy, error) {
	userKey := USER_PREFIX + accountID + ":" + username
	var _u meta.IamUser
	u, err := s.conf.Get(userKey, _u)
	if err != nil || u == nil {
		logger.GetLogger("dedups3").Errorf("failed to get user : %v", err)
		return nil, fmt.Errorf(`failed to get user : %v`, err)
	}

	user, ok := u.(*meta.IamUser)
	if !ok || user == nil {
		logger.GetLogger("dedups3").Errorf("failed to get user : %v", u)
		return nil, fmt.Errorf(`failed to get user : %v`, err)
	}
	policynames := make(meta.StringSet)

	// 直接附加的策略
	for pname := range user.AttachedPolicies {
		policynames[pname] = struct{}{}
	}

	// 通过组附加的策略
	for gname := range user.Groups {
		groupKey := GROUP_PREFIX + accountID + ":" + gname
		var _g meta.IamGroup
		g, err := s.conf.Get(groupKey, _g)
		if err != nil || g == nil {
			logger.GetLogger("dedups3").Errorf("failed to get group : %v", err)
			continue
		}
		group, ok := g.(*meta.IamGroup)
		if !ok || group == nil {
			logger.GetLogger("dedups3").Errorf("failed to get group : %v", g)
			return nil, fmt.Errorf(`failed to get group : %v`, g)
		}
		for pname := range group.AttachedPolicies {
			policynames[pname] = struct{}{}
		}
	}

	// 通过角色附加的策略
	for rname := range user.Roles {
		roleKey := ROLE_PREFIX + accountID + ":" + rname
		var _r meta.IamRole
		r, err := s.conf.Get(roleKey, _r)
		if err != nil || r == nil {
			logger.GetLogger("dedups3").Errorf("failed to get role : %v", err)
			continue
		}
		role, ok := r.(*meta.IamRole)
		if !ok || role == nil {
			logger.GetLogger("dedups3").Errorf("failed to get role : %v", r)
			return nil, fmt.Errorf(`failed to get role : %v`, r)
		}
		for pname := range role.AttachedPolicies {
			policynames[pname] = struct{}{}
		}
	}

	var policies []*meta.IamPolicy
	for pname := range policynames {
		policyKey := POLICY_PREFIX + accountID + ":" + pname
		var _p meta.IamPolicy
		p, err := s.conf.Get(policyKey, _p)
		if err != nil || p == nil {
			logger.GetLogger("dedups3").Errorf("failed to get policy : %v", err)
			continue
		}
		policy, ok := p.(*meta.IamPolicy)
		if !ok || policy == nil {
			logger.GetLogger("dedups3").Errorf("failed to get policy : %v", p)
			return nil, fmt.Errorf(`failed to get policy : %v`, p)
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

// ListUserPermissions 获取用户所有允许的操作
func (s *IamService) ListUserPermissions(accountID, username string) ([]string, error) {
	permissions := make(meta.StringSet)
	allPolicies, err := s.ListUserAllPolicies(accountID, username)
	if err != nil {
		return nil, err
	}
	for _, policy := range allPolicies {
		var doc meta.PolicyDocument
		if err := json.Unmarshal([]byte(policy.Document), &doc); err != nil {
			continue // 跳过无效策略
		}

		for _, stmt := range doc.Statement {
			if stmt.Effect == "Allow" {
				for _, action := range stmt.Action {
					permissions[action] = struct{}{}
				}
			}
		}
	}
	// 转换为切片
	result := make([]string, 0, len(permissions))
	for perm := range permissions {
		result = append(result, perm)
	}
	return result, nil
}

// CheckPermission 检查用户是否有权限执行指定操作
func (s *IamService) CheckPermission(accountID, username, action, resource string) (bool, error) {
	userKey := USER_PREFIX + accountID + ":" + username
	var _u meta.IamUser
	u, err := s.conf.Get(userKey, _u)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, xhttp.ToError(xhttp.ErrAdminNoSuchUser)
	}
	if err != nil || u == nil {
		logger.GetLogger("dedups3").Errorf("failed to get user : %v", err)
		return false, fmt.Errorf(`failed to get user : %v`, err)
	}

	user, ok := u.(*meta.IamUser)
	if !ok || user == nil {
		logger.GetLogger("dedups3").Errorf("failed to get user : %v", u)
		return false, fmt.Errorf(`failed to get user : %v`, err)
	}

	// 1. 根用户拥有所有权限
	if user.IsRoot {
		return true, nil
	}

	// 2. 收集用户所有策略（直接附加+通过组附加+通过角色附加）
	allPolicies, err := s.ListUserAllPolicies(accountID, username)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get user policies : %v", err)
		return false, fmt.Errorf(`failed to get user policies: %v`, err)
	}

	// 3. 检查所有策略
	explicitDenyFound := false
	allowFound := false

	for _, policy := range allPolicies {
		allowed, explicitDeny, err := meta.EvaluatePolicy(policy.Document, action, resource)
		if err != nil {
			return false, fmt.Errorf("error evaluating policy %s: %w", policy.Name, err)
		}

		if explicitDeny {
			explicitDenyFound = true
		}
		if allowed {
			allowFound = true
		}
	}

	// 显式拒绝优先于任何允许
	if explicitDenyFound {
		return false, nil
	}

	return allowFound, nil
}

// IsAllow 简化版的权限检查方法，直接返回布尔值
func (s *IamService) IsAllow(accountID, username, action, resource string) bool {
	allowed, err := s.CheckPermission(accountID, username, action, resource)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("CheckPermission error for user %s: %v", username, err)
		return false
	}
	return allowed
}

func (s *IamService) IsRootUser(accountID, username string) bool {
	return meta.GenerateAccountID(username) == accountID
}
