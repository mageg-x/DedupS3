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
package meta

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mageg-x/boulder/internal/logger"
	Rand "math/rand"
)

// IAMAccount 表示完整的 IAM 系统
type IAMAccount struct {
	AccountID string                `json:"accountId"` // AWS 账户ID
	Users     map[string]*IAMUser   `json:"users"`     // IAM 用户 (key: 用户名)
	Groups    map[string]*IAMGroup  `json:"groups"`    // IAM 用户组 (key: 组名)
	Roles     map[string]*IAMRole   `json:"roles"`     // IAM 角色 (key: 角色名)
	Policies  map[string]*IAMPolicy `json:"policies"`  // IAM 策略 (key: 策略名)
}

// CreateAccount 创建新的 IAM 系统
func CreateAccount() *IAMAccount {
	return &IAMAccount{
		AccountID: generateAccountID(),
		Users:     make(map[string]*IAMUser),
		Groups:    make(map[string]*IAMGroup),
		Roles:     make(map[string]*IAMRole),
		Policies:  make(map[string]*IAMPolicy),
	}
}

// IAMUser 表示 IAM 用户
type IAMUser struct {
	ID                  string            `json:"id"`                  // 用户唯一ID
	ARN                 string            `json:"arn"`                 // 用户ARN
	Username            string            `json:"username"`            // 用户名
	Path                string            `json:"path"`                // 用户路径
	Password            string            `json:"password"`            // 登录密码
	AccessKeys          []AccessKey       `json:"accessKeys"`          // 访问密钥
	MFADevices          []MFADevice       `json:"mfaDevices"`          // MFA设备
	Groups              []string          `json:"groups"`              // 所属用户组
	Roles               []string          `json:"roles"`               // 用户可以担任的角色
	AttachedPolicies    []string          `json:"attachedPolicies"`    // 附加策略
	PermissionsBoundary string            `json:"permissionsBoundary"` // 权限边界
	Tags                map[string]string `json:"tags"`                // 用户标签
	IsRoot              bool              `json:"isRoot"`              // 是否是根用户
	CreatedAt           time.Time         `json:"createdAt"`           // 创建时间
}

// AccessKey 表示访问密钥
type AccessKey struct {
	AccessKeyID     string    `json:"accessKeyId"`
	SecretAccessKey string    `json:"secretAccessKey"`
	Status          string    `json:"status"` // Active | Inactive
	CreatedAt       time.Time `json:"createdAt"`
	ExpiredAt       time.Time `json:"expiredAt"`
	AccountID       string    `json:"accountId"`
	Username        string    `json:"username"` // 用户名
}

// MFADevice 表示 MFA 设备
type MFADevice struct {
	DeviceName string `json:"deviceName"`
	Type       string `json:"type"` // Virtual | Hardware
	Enabled    bool   `json:"enabled"`
}

// IAMGroup 表示 IAM 用户组
type IAMGroup struct {
	ARN              string   `json:"arn"`
	Name             string   `json:"name"`
	Path             string   `json:"path"`
	Users            []string `json:"users"`            // 组成员
	AttachedPolicies []string `json:"attachedPolicies"` // 附加策略
}

// IAMRole 表示 IAM 角色
type IAMRole struct {
	ARN              string   `json:"arn"`
	Name             string   `json:"name"`
	Path             string   `json:"path"`
	AssumeRolePolicy string   `json:"assumeRolePolicy"` // 信任策略
	AttachedPolicies []string `json:"attachedPolicies"` // 附加策略
}

// IAMPolicy 表示 IAM 策略
type IAMPolicy struct {
	ARN         string `json:"arn"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Document    string `json:"document"` // JSON 策略文档
}

// PolicyDocument 表示解析后的策略文档结构
type PolicyDocument struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

// CanonicalUser 表示规范用户（用于 S3 资源所有者）
type CanonicalUser struct {
	ID          string `json:"id"`          // 64字符十六进制ID
	DisplayName string `json:"displayName"` // 显示名称
}

// ============================== ARN 格式化函数 ==============================

func formatUserARN(accountID, path, username string) string {
	return "arn:aws:iam::" + accountID + ":user" + normalizePath(path) + username
}

func formatGroupARN(accountID, path, groupName string) string {
	return "arn:aws:iam::" + accountID + ":group" + normalizePath(path) + groupName
}

func formatRoleARN(accountID, path, roleName string) string {
	return "arn:aws:iam::" + accountID + ":role" + normalizePath(path) + roleName
}

func formatPolicyARN(accountID, policyName string) string {
	return "arn:aws:iam::" + accountID + ":policy/" + policyName
}

func formatRootARN(accountID string) string {
	return "arn:aws:iam::" + accountID + ":root"
}

// 规范化路径格式
func normalizePath(path string) string {
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return path
}

// ============================== 根用户操作 ==============================

// CreateRootUser 创建根用户（账户所有者）
func (a *IAMAccount) CreateRootUser(username, password string) (*IAMUser, error) {
	// 检查是否已存在根用户
	if _, exists := a.Users["root"]; exists {
		logger.GetLogger("boulder").Errorf("root user already exists")
		return nil, errors.New("root user already exists")
	}

	rootUser, err := a.CreateUser(username, password, "/")
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to create root user: %v", err)
		return nil, err
	}
	rootUser.IsRoot = true
	accessKey := generateAccessKeyID()
	secretAccessKey := generateSecretAccessKey()
	rootUser.AccessKeys = append(rootUser.AccessKeys, AccessKey{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretAccessKey,
		Status:          "Active",
		CreatedAt:       time.Now().UTC(),
		ExpiredAt:       time.Now().AddDate(100, 0, 0).UTC(),
		AccountID:       a.AccountID,
		Username:        username,
	})
	a.Users["root"] = rootUser
	return rootUser, nil
}

// ============================== 用户操作 ==============================

// CreateUser 创建新用户
func (a *IAMAccount) CreateUser(username, password, path string) (*IAMUser, error) {
	if u, err := a.GetUser(username); err == nil || u != nil {
		logger.GetLogger("boulder").Errorf("user %s already exists", username)
		return nil, errors.New("user already exists")
	}
	if err := ValidateUsername(username); err != nil {
		logger.GetLogger("boulder").Errorf("username %s is invalid format", username)
		return nil, errors.New("username is invalid format")
	}

	if err := ValidatePassword(password, username); err != nil {
		logger.GetLogger("boulder").Errorf("password for user %s is invalid: %v", username, err)
		return nil, fmt.Errorf("password is invalid: %w", err)
	}

	user := &IAMUser{
		ID:         generateCanonicalUserID(),
		ARN:        formatUserARN(a.AccountID, path, username),
		Username:   username,
		Password:   password,
		Path:       normalizePath(path),
		CreatedAt:  time.Now().UTC(),
		AccessKeys: make([]AccessKey, 0),
		Groups:     make([]string, 0),
		Roles:      make([]string, 0), // 初始化空角色列表
		Tags:       make(map[string]string),
	}

	return user, nil
}

// 根据 用户名获取用户信息，包括root 用户
func (a *IAMAccount) GetUser(username string) (*IAMUser, error) {
	user, uExists := a.Users[username]
	if uExists {
		return user, nil
	}
	rootUser, uExists := a.Users["root"]
	if uExists && rootUser != nil && rootUser.Username == username {
		return rootUser, nil
	}
	return nil, errors.New("user not found")
}

func (a *IAMAccount) GetAllUsers() []*IAMUser {
	users := make([]*IAMUser, 0, len(a.Users))
	for _, user := range a.Users {
		users = append(users, user)
	}
	return users
}

func (a *IAMAccount) GetAllAccessKeys() []*AccessKey {
	keys := make([]*AccessKey, 0)
	for _, user := range a.Users {
		for i := range user.AccessKeys {
			keys = append(keys, &user.AccessKeys[i])
		}
	}
	return keys
}

// AddRoleToUser 让用户可以担任某个角色
func (a *IAMAccount) AddRoleToUser(username, roleName string) error {
	user, uExists := a.Users[username]
	_, rExists := a.Roles[roleName]

	if !uExists || !rExists {
		return errors.New("user or role not found")
	}
	if user.IsRoot {
		return errors.New("root user cannot be added to a role")
	}

	// 检查用户是否已担任该角色
	for _, r := range user.Roles {
		if r == roleName {
			return errors.New("user already has this role")
		}
	}

	user.Roles = append(user.Roles, roleName)
	return nil
}

// DeleteUser 删除用户
func (a *IAMAccount) DeleteUser(username string) error {
	// 检查用户是否存在
	user, exists := a.Users[username]
	if !exists {
		return errors.New("user not found")
	}

	// 检查是否是根用户
	if user.IsRoot {
		return errors.New("cannot delete root user")
	}

	// 从所有组中移除该用户
	for _, groupName := range user.Groups {
		group, gExists := a.Groups[groupName]
		if gExists {
			// 从组中移除用户
			newUsers := make([]string, 0, len(group.Users)-1)
			for _, u := range group.Users {
				if u != username {
					newUsers = append(newUsers, u)
				}
			}
			group.Users = newUsers
		}
	}

	// 删除用户
	delete(a.Users, username)
	return nil
}

// CreateAccessKey 为用户创建访问密钥
func (a *IAMAccount) CreateAccessKey(username string, expiredAt time.Time) (*AccessKey, error) {
	user, exists := a.Users[username]
	if !exists {
		return nil, errors.New("user not found")
	}

	if user.IsRoot {
		return nil, errors.New("root user cannot be added access keys")
	}

	if expiredAt.Before(time.Now()) {
		return nil, errors.New("expired time cannot be before current time")
	}

	accessKey := AccessKey{
		AccessKeyID:     generateAccessKeyID(),
		SecretAccessKey: generateSecretAccessKey(),
		Status:          "Active",
		CreatedAt:       time.Now().UTC(),
		ExpiredAt:       expiredAt,
		Username:        username,
		AccountID:       a.AccountID,
	}

	user.AccessKeys = append(user.AccessKeys, accessKey)
	return &accessKey, nil
}

// ============================== 组操作 ==============================

// CreateGroup 创建新用户组
func (a *IAMAccount) CreateGroup(caller *IAMUser, name, path string) (*IAMGroup, error) {
	if !a.canManageIAM(caller) {
		return nil, errors.New("unauthorized to create groups")
	}

	if _, exists := a.Groups[name]; exists {
		return nil, errors.New("group already exists")
	}

	group := &IAMGroup{
		ARN:  formatGroupARN(a.AccountID, path, name),
		Name: name,
		Path: normalizePath(path),
	}

	a.Groups[name] = group
	return group, nil
}

// DeleteGroup 删除用户组
func (a *IAMAccount) DeleteGroup(groupName string) error {
	// 检查组是否存在
	group, exists := a.Groups[groupName]
	if !exists {
		return errors.New("group not found")
	}

	// 从组成员的用户中移除该组
	for _, username := range group.Users {
		user, uExists := a.Users[username]
		if uExists {
			newGroups := make([]string, 0, len(user.Groups)-1)
			for _, g := range user.Groups {
				if g != groupName {
					newGroups = append(newGroups, g)
				}
			}
			user.Groups = newGroups
		}
	}

	// 删除组
	delete(a.Groups, groupName)
	return nil
}

// AddUserToGroup 添加用户到组
func (a *IAMAccount) AddUserToGroup(username, groupName string) error {
	user, uExists := a.Users[username]
	group, gExists := a.Groups[groupName]

	if !uExists || !gExists {
		return errors.New("user or group not found")
	}

	if user.IsRoot {
		return errors.New("root user cannot be added to a group")
	}

	// 检查用户是否已在组中
	for _, g := range user.Groups {
		if g == groupName {
			return errors.New("user already in group")
		}
	}

	user.Groups = append(user.Groups, groupName)
	group.Users = append(group.Users, username)
	return nil
}

// ============================== 策略操作 ==============================

// CreatePolicy 创建新策略
func (a *IAMAccount) CreatePolicy(caller *IAMUser, name, description, document string) (*IAMPolicy, error) {
	// 检查调用者是否有创建策略的权限
	if !a.canManageIAM(caller) {
		return nil, errors.New("unauthorized to create policies")
	}

	if _, exists := a.Policies[name]; exists {
		return nil, errors.New("policy already exists")
	}

	// 验证策略文档
	if err := ValidatePolicyDocument(document); err != nil {
		return nil, fmt.Errorf("invalid policy document: %w", err)
	}

	policy := &IAMPolicy{
		ARN:         formatPolicyARN(a.AccountID, name),
		Name:        name,
		Description: description,
		Document:    document,
	}

	a.Policies[name] = policy
	return policy, nil
}

// DeletePolicy 删除策略
func (a *IAMAccount) DeletePolicy(policyName string) error {
	// 检查策略是否存在
	_, exists := a.Policies[policyName]
	if !exists {
		return errors.New("policy not found")
	}

	// 从所有用户中移除该策略
	for _, user := range a.Users {
		newPolicies := make([]string, 0, len(user.AttachedPolicies))
		for _, p := range user.AttachedPolicies {
			if p != policyName {
				newPolicies = append(newPolicies, p)
			}
		}
		user.AttachedPolicies = newPolicies
	}

	// 从所有组中移除该策略
	for _, group := range a.Groups {
		newPolicies := make([]string, 0, len(group.AttachedPolicies))
		for _, p := range group.AttachedPolicies {
			if p != policyName {
				newPolicies = append(newPolicies, p)
			}
		}
		group.AttachedPolicies = newPolicies
	}

	// 从所有角色中移除该策略
	for _, role := range a.Roles {
		newPolicies := make([]string, 0, len(role.AttachedPolicies))
		for _, p := range role.AttachedPolicies {
			if p != policyName {
				newPolicies = append(newPolicies, p)
			}
		}
		role.AttachedPolicies = newPolicies
	}

	// 删除策略
	delete(a.Policies, policyName)
	return nil
}

// AttachPolicyToUser 附加策略到用户
func (a *IAMAccount) AttachPolicyToUser(caller *IAMUser, username, policyName string) error {
	// 检查特定 IAM 权限
	allowed, _ := a.CheckPermission(caller.Username, "iam:AttachUserPolicy", "*") // 检查调用者是否有附加策略的权限
	if !allowed {
		return errors.New("unauthorized to attach policies")
	}

	user, exists := a.Users[username]
	if !exists {
		return errors.New("user not found")
	}

	if user.IsRoot {
		return errors.New("cannot attach policies to root user")
	}

	// 检查策略是否存在
	if _, pExists := a.Policies[policyName]; !pExists {
		return errors.New("policy not found")
	}

	// 检查是否已附加
	for _, p := range user.AttachedPolicies {
		if p == policyName {
			return errors.New("policy already attached")
		}
	}

	user.AttachedPolicies = append(user.AttachedPolicies, policyName)
	return nil
}

// canManageIAM 检查用户是否有 IAM 管理权限
func (a *IAMAccount) canManageIAM(user *IAMUser) bool {
	// 根用户总是有权限
	if user.IsRoot {
		return true
	}

	// 检查用户是否有 IAM 管理策略
	allowed, _ := a.CheckPermission(user.Username, "iam:*", "*")
	return allowed
}

// ============================== 角色操作 ==============================

// CreateRole 创建新角色
func (a *IAMAccount) CreateRole(caller *IAMUser, name, path, assumeRolePolicy string) (*IAMRole, error) {
	if !a.canManageIAM(caller) {
		return nil, errors.New("unauthorized to create roles")
	}

	if _, exists := a.Roles[name]; exists {
		return nil, errors.New("role already exists")
	}

	// 验证信任策略是否是有效的 JSON
	if !isValidJSON(assumeRolePolicy) {
		return nil, errors.New("assumeRolePolicy must be valid JSON")
	}

	role := &IAMRole{
		ARN:              formatRoleARN(a.AccountID, path, name),
		Name:             name,
		Path:             normalizePath(path),
		AssumeRolePolicy: assumeRolePolicy,
		AttachedPolicies: make([]string, 0),
	}

	a.Roles[name] = role
	return role, nil
}

// AttachPolicyToRole 附加策略到角色
func (a *IAMAccount) AttachPolicyToRole(roleName, policyName string) error {
	role, exists := a.Roles[roleName]
	if !exists {
		return errors.New("role not found")
	}

	// 检查策略是否存在
	if _, pExists := a.Policies[policyName]; !pExists {
		return errors.New("policy not found")
	}

	// 检查是否已附加
	for _, p := range role.AttachedPolicies {
		if p == policyName {
			return errors.New("policy already attached to role")
		}
	}

	role.AttachedPolicies = append(role.AttachedPolicies, policyName)
	return nil
}

// UpdateAssumeRolePolicy 更新角色的信任策略
func (a *IAMAccount) UpdateAssumeRolePolicy(roleName, assumeRolePolicy string) error {
	role, exists := a.Roles[roleName]
	if !exists {
		return errors.New("role not found")
	}

	// 验证新的信任策略
	if !isValidJSON(assumeRolePolicy) {
		return errors.New("assumeRolePolicy must be valid JSON")
	}

	role.AssumeRolePolicy = assumeRolePolicy
	return nil
}

// ListAttachedRolePolicies 列出角色附加的策略
func (a *IAMAccount) ListAttachedRolePolicies(roleName string) ([]string, error) {
	role, exists := a.Roles[roleName]
	if !exists {
		return nil, errors.New("role not found")
	}
	return role.AttachedPolicies, nil
}

// DeleteRole 删除角色
func (a *IAMAccount) DeleteRole(roleName string) error {
	// 检查角色是否存在
	_, exists := a.Roles[roleName]
	if !exists {
		return errors.New("role not found")
	}

	// 从所有用户中移除该角色
	for _, user := range a.Users {
		newRoles := make([]string, 0, len(user.Roles))
		for _, r := range user.Roles {
			if r != roleName {
				newRoles = append(newRoles, r)
			}
		}
		user.Roles = newRoles
	}

	// 删除角色
	delete(a.Roles, roleName)
	return nil
}

// DetachPolicyFromRole 从角色分离策略
func (a *IAMAccount) DetachPolicyFromRole(roleName, policyName string) error {
	role, exists := a.Roles[roleName]
	if !exists {
		return errors.New("role not found")
	}

	newPolicies := make([]string, 0, len(role.AttachedPolicies))
	found := false

	for _, p := range role.AttachedPolicies {
		if p == policyName {
			found = true
		} else {
			newPolicies = append(newPolicies, p)
		}
	}

	if !found {
		return errors.New("policy not attached to role")
	}

	role.AttachedPolicies = newPolicies
	return nil
}

// GetRole 获取角色信息
func (a *IAMAccount) GetRole(roleName string) (*IAMRole, error) {
	role, exists := a.Roles[roleName]
	if !exists {
		return nil, errors.New("role not found")
	}
	return role, nil
}

// ============================== 权限检查方法 ==============================

// CheckPermission 检查用户是否有权限执行指定操作
func (a *IAMAccount) CheckPermission(username, action, resource string) (bool, error) {
	user, exists := a.Users[username]
	if !exists {
		return false, errors.New("user not found")
	}

	// 1. 根用户拥有所有权限
	if user.IsRoot {
		return true, nil
	}

	// 2. 收集用户所有策略（直接附加+通过组附加）
	allPolicies := a.getUserAllPolicies(user)

	// 3. 检查权限边界（如果设置了）
	if user.PermissionsBoundary != "" {
		if boundaryPolicy, exists := a.Policies[user.PermissionsBoundary]; exists {
			allowedByBoundary, explicitDeny, err := evaluatePolicy(boundaryPolicy.Document, action, resource)
			if err != nil {
				return false, fmt.Errorf("error evaluating permissions boundary: %w", err)
			}
			if explicitDeny {
				return false, nil // 权限边界明确拒绝
			}
			if !allowedByBoundary {
				return false, nil // 权限边界未允许此操作
			}
		}
	}

	// 4. 检查所有策略
	explicitDenyFound := false
	allowFound := false

	for _, policy := range allPolicies {
		allowed, explicitDeny, err := evaluatePolicy(policy.Document, action, resource)
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

// 获取用户所有策略（直接附加+通过组附加）
func (a *IAMAccount) getUserAllPolicies(user *IAMUser) []*IAMPolicy {
	var policies []*IAMPolicy

	// 直接附加的策略
	for _, policyName := range user.AttachedPolicies {
		if policy, exists := a.Policies[policyName]; exists {
			policies = append(policies, policy)
		}
	}

	// 通过组附加的策略
	for _, groupName := range user.Groups {
		if group, exists := a.Groups[groupName]; exists {
			for _, policyName := range group.AttachedPolicies {
				if policy, exists := a.Policies[policyName]; exists {
					policies = append(policies, policy)
				}
			}
		}
	}

	// 通过角色附加的策略（新增部分）
	for _, roleName := range user.Roles {
		if role, exists := a.Roles[roleName]; exists {
			for _, policyName := range role.AttachedPolicies {
				if policy, exists := a.Policies[policyName]; exists {
					policies = append(policies, policy)
				}
			}
		}
	}
	return policies
}

// 评估单个策略
func evaluatePolicy(policyDoc, action, resource string) (allowed, explicitDeny bool, err error) {
	var doc PolicyDocument
	if err := json.Unmarshal([]byte(policyDoc), &doc); err != nil {
		return false, false, fmt.Errorf("invalid policy document: %w", err)
	}

	for _, stmt := range doc.Statement {
		// 检查操作匹配
		actionMatch := false
		for _, a := range stmt.Action {
			if matchPattern(a, action) {
				actionMatch = true
				break
			}
		}
		if !actionMatch {
			continue
		}

		// 检查资源匹配（如果策略指定了资源）
		if len(stmt.Resource) > 0 {
			resourceMatch := false
			for _, r := range stmt.Resource {
				if matchPattern(r, resource) {
					resourceMatch = true
					break
				}
			}
			if !resourceMatch {
				continue
			}
		}

		// 匹配的语句
		switch stmt.Effect {
		case "Allow":
			allowed = true
		case "Deny":
			explicitDeny = true
			return false, true, nil // 遇到显式拒绝立即返回
		}
	}

	return allowed, explicitDeny, nil
}

// ============================== 辅助权限方法 ==============================

// CanUserPerformAction 检查用户是否能执行操作（简化版）
func (a *IAMAccount) CanUserPerformAction(username, action string) bool {
	// 对于不关心具体资源的情况，使用通配符资源
	allowed, _ := a.CheckPermission(username, action, "*")
	return allowed
}

// ListUserPermissions 列出用户所有允许的操作
func (a *IAMAccount) ListUserPermissions(username string) ([]string, error) {
	user, exists := a.Users[username]
	if !exists {
		return nil, errors.New("user not found")
	}

	permissions := make(map[string]struct{})
	allPolicies := a.getUserAllPolicies(user)

	for _, policy := range allPolicies {
		var doc PolicyDocument
		if err := json.Unmarshal([]byte(policy.Document), &doc); err != nil {
			continue // 跳过无效策略
		}

		for _, stmt := range doc.Statement {
			if stmt.Effect == "Allow" {
				for _, action := range stmt.Action {
					// 解析通配符操作
					if strings.Contains(action, "*") {
						// 简化处理：添加通配符模式本身
						permissions[action] = struct{}{}
					} else {
						permissions[action] = struct{}{}
					}
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

// ============================== 认证操作 ==============================

// Authenticate 认证用户
func (a *IAMAccount) Authenticate(accessKeyID, secretAccessKey string) (*IAMUser, error) {
	for _, user := range a.Users {
		for _, key := range user.AccessKeys {
			if key.AccessKeyID == accessKeyID &&
				key.SecretAccessKey == secretAccessKey &&
				key.Status == "Active" {
				return user, nil
			}
		}
	}

	return nil, errors.New("authentication failed")
}

// GetUserByAccessKeyID 通过访问密钥ID查找用户
func (a *IAMAccount) GetUserByAccessKeyID(accessKeyID string) (*IAMUser, error) {
	for _, user := range a.Users {
		for _, key := range user.AccessKeys {
			if key.AccessKeyID == accessKeyID {
				return user, nil
			}
		}
	}
	return nil, errors.New("access key not found")
}

// ============================== 辅助函数 ==============================

// 验证JSON格式
func isValidJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}

// ValidatePolicyDocument 验证策略文档是否有效
func ValidatePolicyDocument(document string) error {
	var doc PolicyDocument
	if err := json.Unmarshal([]byte(document), &doc); err != nil {
		return fmt.Errorf("invalid JSON format: %w", err)
	}

	if doc.Version != "2008-10-17" && doc.Version != "2012-10-17" {
		return errors.New("unsupported policy version, must be '2008-10-17' or '2012-10-17'")
	}

	for i, stmt := range doc.Statement {
		if stmt.Effect != "Allow" && stmt.Effect != "Deny" {
			return fmt.Errorf("statement %d has invalid effect '%s', must be 'Allow' or 'Deny'", i, stmt.Effect)
		}

		if len(stmt.Action) == 0 {
			return fmt.Errorf("statement %d must specify at least one action", i)
		}
	}

	return nil
}

// 生成账户ID (12位数字，符合AWS规范)
func generateAccountID() string {
	const charset = "0123456789"
	b := make([]byte, 12)
	for i := range b {
		b[i] = charset[Rand.Intn(len(charset))]
	}
	return string(b)
}

// 生成规范用户ID (64字符十六进制)
func generateCanonicalUserID() string {
	bytes := make([]byte, 32)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// ValidateAccountID 验证账户ID是否符合AWS规范 (12位数字)
func ValidateAccountID(accountID string) error {
	if len(accountID) != 12 {
		return errors.New("account ID must be 12 digits")
	}

	for _, c := range accountID {
		if c < '0' || c > '9' {
			return errors.New("account ID must contain only digits")
		}
	}

	return nil
}

// ValidateUsername 验证用户名是否符合AWS IAM规范
// 用户名规则:
// 1. 长度在1-64字符之间
// 2. 只能包含字母、数字、下划线(_)、点(.)和连字符(-)
// 3. 不能以点(.)或连字符(-)开头或结尾
// 4. 不能连续使用点(.)或连字符(-)
func ValidateUsername(username string) error {
	if len(username) == 0 || len(username) > 64 || username == "root" {
		return errors.New("username length must be between 1 and 64 characters")
	}

	// 检查首字符和尾字符
	firstChar := username[0]
	lastChar := username[len(username)-1]
	if firstChar == '.' || firstChar == '-' || lastChar == '.' || lastChar == '-' {
		return errors.New("username cannot start or end with '.' or '-'")
	}

	// 检查字符是否合法以及是否有连续的点或连字符
	for i := 0; i < len(username)-1; i++ {
		current := username[i]
		next := username[i+1]

		// 检查字符是否合法
		if !((current >= 'a' && current <= 'z') ||
			(current >= 'A' && current <= 'Z') ||
			(current >= '0' && current <= '9') ||
			current == '_' || current == '.' || current == '-') {
			return errors.New("username can only contain letters, numbers, '_', '.' and '-' ")
		}

		// 检查是否有连续的点或连字符
		if (current == '.' && next == '.') || (current == '-' && next == '-') {
			return errors.New("username cannot contain consecutive '.' or '-'")
		}
	}

	// 检查最后一个字符是否合法
	lastChar = username[len(username)-1]
	if !((lastChar >= 'a' && lastChar <= 'z') ||
		(lastChar >= 'A' && lastChar <= 'Z') ||
		(lastChar >= '0' && lastChar <= '9') ||
		lastChar == '_' || lastChar == '.' || lastChar == '-') {
		return errors.New("username can only contain letters, numbers, '_', '.' and '-' ")
	}

	return nil
}

// ValidatePassword 验证密码是否符合AWS IAM规范
// 密码规则:
// 1. 长度至少为8个字符
// 2. 包含至少一个大写字母
// 3. 包含至少一个小写字母
// 4. 包含至少一个数字
// 5. 包含至少一个特殊字符 (!@#$%^&*()_+-=[]{}|;:,.<>?/)
// 6. 不能包含用户名
func ValidatePassword(password, username string) error {
	// 检查长度
	if len(password) < 8 {
		return errors.New("password must be at least 8 characters long")
	}

	// 检查是否包含大写字母
	hasUpperCase := false
	// 检查是否包含小写字母
	hasLowerCase := false
	// 检查是否包含数字
	hasDigit := false
	// 检查是否包含特殊字符
	hasSpecialChar := false
	// 特殊字符集
	specialChars := "!@#$%^&*()_+-=[]{}|;:,.<>?/"

	for _, c := range password {
		if c >= 'A' && c <= 'Z' {
			hasUpperCase = true
		} else if c >= 'a' && c <= 'z' {
			hasLowerCase = true
		} else if c >= '0' && c <= '9' {
			hasDigit = true
		} else if strings.ContainsRune(specialChars, c) {
			hasSpecialChar = true
		}
	}

	if !hasUpperCase {
		return errors.New("password must contain at least one uppercase letter")
	}

	if !hasLowerCase {
		return errors.New("password must contain at least one lowercase letter")
	}

	if !hasDigit {
		return errors.New("password must contain at least one digit")
	}

	if !hasSpecialChar {
		return errors.New("password must contain at least one special character (!@#$%^&*()_+-=[]{}|;:,.<>?/)")
	}

	// 检查是否包含用户名
	if strings.Contains(strings.ToLower(password), strings.ToLower(username)) {
		return errors.New("password cannot contain username")
	}

	return nil
}

func ValidatePath(path string) bool {
	// 空路径是有效的
	if path == "" {
		return true
	}

	// 路径必须以/开头和结尾
	if !strings.HasPrefix(path, "/") || !strings.HasSuffix(path, "/") {
		return false
	}

	// 检查路径中的字符是否有效
	for _, c := range path {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '/' || c == '.' || c == '-' || c == '_' {
			continue
		}
		return false
	}

	// 检查是否包含连续的/
	if strings.Contains(path, "//") {
		return false
	}

	return true
}

// 生成访问密钥ID (20字符)
func generateAccessKeyID() string {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 20)
	for i := range b {
		b[i] = charset[Rand.Intn(len(charset))]
	}
	return string(b)
}

// 生成秘密访问密钥 (40字符)
func generateSecretAccessKey() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"
	b := make([]byte, 40)
	for i := range b {
		b[i] = charset[Rand.Intn(len(charset))]
	}
	return string(b)
}
