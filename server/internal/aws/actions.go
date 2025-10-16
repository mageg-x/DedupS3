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
package aws

// Action 枚举类型
type Action string

const (
	// S3 Actions
	GetObject                   Action = "s3:GetObject"
	HeadObject                         = "s3:HeadObject"
	PutObject                          = "s3:PutObject"
	RestoreObject                      = "s3:RestoreObject"
	DeleteObject                       = "s3:DeleteObject"
	ListMultipartUploadParts           = "s3:ListMultipartUploadParts"
	AbortMultipartUpload               = "s3:AbortMultipartUpload"
	GetObjectAttributes                = "s3:GetObjectAttributes"
	GetObjectAcl                       = "s3:GetObjectAcl"
	PutObjectAcl                       = "s3:PutObjectAcl"
	GetBucketAcl                       = "s3:GetBucketAcl"
	PutBucketAcl                       = "s3:PutBucketAcl"
	GetObjectTagging                   = "s3:GetObjectTagging"
	PutObjectTagging                   = "s3:PutObjectTagging"
	DeleteObjectTagging                = "s3:DeleteObjectTagging"
	GetBucketTagging                   = "s3:GetBucketTagging"
	PutBucketTagging                   = "s3:PutBucketTagging"
	DeleteBucketTagging                = "s3:DeleteBucketTagging"
	ListBucketVersions                 = "s3:ListBucketVersions"
	ListBucket                         = "s3:ListBucket"
	ListAllMyBuckets                   = "s3:ListAllMyBuckets"
	GetBucketLocation                  = "s3:GetBucketLocation"
	GetLifecycleConfiguration          = "s3:GetLifecycleConfiguration"
	PutLifecycleConfiguration          = "s3:PutLifecycleConfiguration"
	GetReplicationConfiguration        = "s3:GetReplicationConfiguration"
	PutReplicationConfiguration        = "s3:PutReplicationConfiguration"
	GetEncryptionConfiguration         = "s3:GetEncryptionConfiguration"
	PutEncryptionConfiguration         = "s3:PutEncryptionConfiguration"
	GetObjectLockConfiguration         = "s3:GetObjectLockConfiguration"
	PutObjectLockConfiguration         = "s3:PutObjectLockConfiguration"
	GetObjectRetention                 = "s3:GetObjectRetention"
	PutObjectRetention                 = "s3:PutObjectRetention"
	GetObjectLegalHold                 = "s3:GetObjectLegalHold"
	PutObjectLegalHold                 = "s3:PutObjectLegalHold"
	GetBucketNotification              = "s3:GetBucketNotification"
	PutBucketNotification              = "s3:PutBucketNotification"
	GetBucketCORS                      = "s3:GetBucketCORS"
	PutBucketCORS                      = "s3:PutBucketCORS"
	DeleteBucketCORS                   = "s3:DeleteBucketCORS"
	GetBucketWebsite                   = "s3:GetBucketWebsite"
	PutBucketWebsite                   = "s3:PutBucketWebsite"
	DeleteBucketWebsite                = "s3:DeleteBucketWebsite"
	GetBucketPolicy                    = "s3:GetBucketPolicy"
	PutBucketPolicy                    = "s3:PutBucketPolicy"
	DeleteBucketPolicy                 = "s3:DeleteBucketPolicy"
	GetBucketPolicyStatus              = "s3:GetBucketPolicyStatus"
	GetBucketVersioning                = "s3:GetBucketVersioning"
	PutBucketVersioning                = "s3:PutBucketVersioning"
	GetAccelerateConfiguration         = "s3:GetAccelerateConfiguration"
	GetRequestPayment                  = "s3:GetRequestPayment"
	GetBucketLogging                   = "s3:GetBucketLogging"
	PutBucketLogging                   = "s3:PutBucketLogging"
	ListBucketMultipartUploads         = "s3:ListBucketMultipartUploads"
	SelectObjectContent                = "s3:SelectObjectContent"

	// IAM Actions
	CreateAccount   = "iam:CreateAccount"
	GetAccount      = "iam:GetAccount"
	UpdateAccount   = "iam:UpdateAccount"
	DeleteAccount   = "iam:DeleteAccount"
	CreateUser      = "iam:CreateUser"
	DeleteUser      = "iam:DeleteUser"
	CreateAccessKey = "iam:CreateAccessKey"
	DeleteAccessKey = "iam:DeleteAccessKey"
	GetAccessKey    = "iam:GetAccessKey"
	CreatePolicy    = "iam:CreatePolicy"
	DeletePolicy    = "iam:DeletePolicy"
	GetPolicy       = "iam:GetPolicy"
	UpdatePolicy    = "iam:UpdatePolicy"
	ListPolicy      = "iam:ListPolicy"

	// Console Actions
	AdminLogin      = "console:Login"
	AdminStats      = "console:Stats"
	AdminBucket     = "console:Bucket"
	AdminAccessKey  = "console:AccessKey"
	AdminIAM        = "console:Iam"
	AdminUser       = "console:User"
	AdminRole       = "console:Role"
	AdminGroup      = "console:Group"
	AdminPolicy     = "console:Policy"
	AdminEvent      = "console:Event"
	AdminAudit      = "console:Audit"
	AdminConfigure  = "console:Configure"
	AdminStorage    = "console:Storage"
	AdminQuota      = "console:Quota"
	AdminChunk      = "console:Chunk"
	AdminAdvance    = "console:Advance"
	AdminMigrate    = "console:Migrate"
	AdminDefragment = "console:Defragment"
	AdminSnapshot   = "console:Snapshot"
	AdminAnalysis   = "console:Analysis"
	AdminDebug      = "console:Debug"
)

// 各类别支持的动作集合（set）
var SupportedS3Actions = map[Action]struct{}{
	GetObject:                   {},
	HeadObject:                  {},
	PutObject:                   {},
	RestoreObject:               {},
	DeleteObject:                {},
	ListMultipartUploadParts:    {},
	AbortMultipartUpload:        {},
	GetObjectAttributes:         {},
	GetObjectAcl:                {},
	PutObjectAcl:                {},
	GetBucketAcl:                {},
	PutBucketAcl:                {},
	GetObjectTagging:            {},
	PutObjectTagging:            {},
	DeleteObjectTagging:         {},
	GetBucketTagging:            {},
	PutBucketTagging:            {},
	DeleteBucketTagging:         {},
	ListBucketVersions:          {},
	ListBucket:                  {},
	ListAllMyBuckets:            {},
	GetBucketLocation:           {},
	GetLifecycleConfiguration:   {},
	PutLifecycleConfiguration:   {},
	GetReplicationConfiguration: {},
	PutReplicationConfiguration: {},
	GetEncryptionConfiguration:  {},
	PutEncryptionConfiguration:  {},
	GetObjectLockConfiguration:  {},
	PutObjectLockConfiguration:  {},
	GetObjectRetention:          {},
	PutObjectRetention:          {},
	GetObjectLegalHold:          {},
	PutObjectLegalHold:          {},
	GetBucketNotification:       {},
	PutBucketNotification:       {},
	GetBucketCORS:               {},
	PutBucketCORS:               {},
	DeleteBucketCORS:            {},
	GetBucketWebsite:            {},
	PutBucketWebsite:            {},
	DeleteBucketWebsite:         {},
	GetBucketPolicy:             {},
	PutBucketPolicy:             {},
	DeleteBucketPolicy:          {},
	GetBucketPolicyStatus:       {},
	GetBucketVersioning:         {},
	PutBucketVersioning:         {},
	GetAccelerateConfiguration:  {},
	GetRequestPayment:           {},
	GetBucketLogging:            {},
	PutBucketLogging:            {},
	ListBucketMultipartUploads:  {},
	SelectObjectContent:         {},
}

var SupportedIamActions = map[Action]struct{}{
	CreateAccount:   {},
	GetAccount:      {},
	UpdateAccount:   {},
	DeleteAccount:   {},
	CreateUser:      {},
	DeleteUser:      {},
	CreateAccessKey: {},
	DeleteAccessKey: {},
	GetAccessKey:    {},
	CreatePolicy:    {},
	DeletePolicy:    {},
	GetPolicy:       {},
	UpdatePolicy:    {},
	ListPolicy:      {},
}

var SupportedAdminActions = map[Action]struct{}{
	AdminLogin:      {},
	AdminStats:      {},
	AdminBucket:     {},
	AdminAccessKey:  {},
	AdminIAM:        {},
	AdminUser:       {},
	AdminRole:       {},
	AdminGroup:      {},
	AdminPolicy:     {},
	AdminEvent:      {},
	AdminAudit:      {},
	AdminConfigure:  {},
	AdminStorage:    {},
	AdminQuota:      {},
	AdminChunk:      {},
	AdminAdvance:    {},
	AdminMigrate:    {},
	AdminDefragment: {},
	AdminSnapshot:   {},
	AdminAnalysis:   {},
	AdminDebug:      {},
}

// SupportedActions 所有支持的动作（总集合）
var SupportedActions map[Action]struct{}

// init 函数：在程序启动时初始化总集合
func init() {
	SupportedActions = make(map[Action]struct{})

	// 合并 S3 动作
	for action := range SupportedS3Actions {
		SupportedActions[action] = struct{}{}
	}

	// 合并 IAM 动作
	for action := range SupportedIamActions {
		SupportedActions[action] = struct{}{}
	}

	// 合并 Admin 动作
	for action := range SupportedAdminActions {
		SupportedActions[action] = struct{}{}
	}
}

// AllActions 返回所有支持的 Action 列表（slice 形式）
func AllActions() []Action {
	actions := make([]Action, 0, len(SupportedActions))
	for action := range SupportedActions {
		actions = append(actions, action)
	}
	return actions
}

// String 实现 fmt.Stringer 接口（可选）
func (a Action) String() string {
	return string(a)
}
