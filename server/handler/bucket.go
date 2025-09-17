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
package handler

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gorilla/mux"

	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
	sb "github.com/mageg-x/boulder/service/bucket"
)

type ListAllMyBucketsResult struct {
	XMLName xml.Name       `xml:"ListAllMyBucketsResult"`
	XMLNS   string         `xml:"xmlns,attr"` // 固定值为http://s3.amazonaws.com/doc/2006-03-01/
	Owner   types.Owner    `xml:"Owner"`
	Buckets []types.Bucket `xml:"Buckets>Bucket"`
}

// GetReqVar 获取请求中一些通用的变量
func GetReqVar(r *http.Request) (string, string, string, string) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]
	object = utils.TrimLeadingSlash(object)
	if object != "" {
		// 处理多个连续的斜杠（可选，根据业务需求）
		object = regexp.MustCompile(`/+`).ReplaceAllString(object, "/")
		// 移除开头的斜杠（如果有）
		object = strings.TrimPrefix(object, "/")
	}

	// 从请求上下文获取变量
	ctx := r.Context()
	// 获取accessKeyID
	accessKeyID, ok := ctx.Value("accesskey").(string)
	if !ok {
		logger.GetLogger("boulder").Errorf("Failed to get accessKeyID from context")
	} else {
		logger.GetLogger("boulder").Tracef("accessKeyID from context: %s", accessKeyID)
	}
	// 获取region
	region, ok := ctx.Value("region").(string)
	if !ok {
		logger.GetLogger("boulder").Errorf("Failed to get region from context")
	} else {
		logger.GetLogger("boulder").Tracef("region from context: %s", region)
	}

	return bucket, object, region, accessKeyID
}

// ListBucketsHandler 列出所有存储桶
func ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: ListBucketsHandler")
	_, _, _, accessKeyID := GetReqVar(r)
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	buckets, owner, err := bs.ListBuckets(&sb.BaseBucketParams{
		AccessKeyID: accessKeyID,
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to list buckets: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
	}

	var bucketList []types.Bucket
	for _, bucket := range buckets {
		bucketList = append(bucketList, types.Bucket{
			Name:         &bucket.Name,
			CreationDate: &bucket.CreationDate,
		})
	}

	result := ListAllMyBucketsResult{
		XMLName: xml.Name{Local: "ListAllMyBucketsResult"},
		XMLNS:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: types.Owner{
			DisplayName: &owner.DisplayName,
			ID:          &owner.ID,
		},
		Buckets: bucketList,
	}

	xhttp.WriteAWSSuc(w, r, result)
}

// GetBucketLocationHandler 处理 GET Bucket Location 请求
func GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketLocationHandler")
	bucket, _, region, accessKeyID := GetReqVar(r)

	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	_bucket, err := bs.GetBucketInfo(&sb.BaseBucketParams{
		BucketName:  bucket,
		AccessKeyID: accessKeyID,
		Location:    region,
	})

	if err != nil || _bucket == nil {
		logger.GetLogger("boulder").Errorf("bucket %s does not exist: %v", bucket, err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
			return
		}
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	// 创建符合S3标准的响应结构
	locationResult := sb.GetBucketLocationResult{
		XMLName: xml.Name{Local: "LocationConstraint"},
		XMLNS:   "http://s3.amazonaws.com/doc/2006-03-01/",
	}
	// 对于默认区域（如us-east-1），Location字段应该为空
	if _bucket.Location != "" && _bucket.Location != "us-east-1" {
		locationResult.Location = _bucket.Location
	}

	// 写入成功响应
	xhttp.WriteAWSSuc(w, r, locationResult)
}

// GetBucketPolicyHandler 处理 GET Bucket Policy 请求
func GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketPolicyHandler")

	// 获取请求变量
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取x-amz-expected-bucket-owner头部
	expectedOwnerID := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwnerID = strings.TrimSpace(expectedOwnerID)

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 获取桶信息
	bucketInfo, err := bs.GetBucketInfo(&sb.BaseBucketParams{
		BucketName:  bucket,
		Location:    region,
		AccessKeyID: accessKeyID,
	})
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else {
			logger.GetLogger("boulder").Errorf("failed to get bucket info: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 检查expectedOwnerID是否匹配
	if expectedOwnerID != "" && bucketInfo.Owner.ID != expectedOwnerID {
		logger.GetLogger("boulder").Errorf("bucket owner mismatch: expected %s, got %s", expectedOwnerID, bucketInfo.Owner.ID)
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}

	// 检查桶是否有策略
	if bucketInfo.Policy == nil {
		logger.GetLogger("boulder").Errorf("bucket %s has no policy", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucketPolicy)
		return
	}

	// 验证策略格式
	if err := bucketInfo.Policy.Validate(); err != nil {
		logger.GetLogger("boulder").Errorf("invalid bucket policy: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	// 转换策略为JSON并返回
	// 创建一个不包含内部时间戳的策略副本用于响应
	responsePolicy := &meta.BucketPolicy{
		ID:         bucketInfo.Policy.ID,
		Version:    bucketInfo.Policy.Version,
		Statements: bucketInfo.Policy.Statements,
	}

	// 使用JSON响应函数返回策略
	xhttp.WriteAWSJSONSuc(w, r, responsePolicy)
	logger.GetLogger("boulder").Infof("successfully retrieved bucket policy for bucket: %s", bucket)
}

// GetBucketLifecycleHandler 处理 GET Bucket Lifecycle 请求
func GetBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketLifecycleHandler")

	// 获取请求变量
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取x-amz-expected-bucket-owner头部
	expectedOwnerID := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwnerID = strings.TrimSpace(expectedOwnerID)

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 获取桶信息
	bucketInfo, err := bs.GetBucketInfo(&sb.BaseBucketParams{
		BucketName:  bucket,
		Location:    region,
		AccessKeyID: accessKeyID,
	})
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else {
			logger.GetLogger("boulder").Errorf("failed to get bucket info: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	if expectedOwnerID != "" && expectedOwnerID != bucketInfo.Owner.ID {
		logger.GetLogger("boulder").Errorf("bucket owner mismatch: expected %s, got %s", expectedOwnerID, bucketInfo.Owner.ID)
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
	}

	// 检查生命周期配置是否存在
	if bucketInfo.Lifecycle == nil || len(bucketInfo.Lifecycle.Rules) == 0 {
		// 如果不存在生命周期配置，返回空的LifecycleConfiguration
		bucketInfo.Lifecycle = &meta.LifecycleConfiguration{}
	}

	bucketInfo.Lifecycle.XMLName = xml.Name{Local: "LifecycleConfiguration"}
	bucketInfo.Lifecycle.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
	// 返回生命周期配置
	xhttp.WriteAWSSuc(w, r, bucketInfo.Lifecycle)
	logger.GetLogger("boulder").Tracef("successfully retrieved lifecycle configuration for bucket: %s", bucket)
}

// GetBucketEncryptionHandler 处理 GET Bucket Encryption 请求
func GetBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketEncryptionHandler")
	// TODO: 实现 GET Bucket Encryption 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetBucketObjectLockConfigHandler 处理 GET Bucket Object Lock Configuration 请求
func GetBucketObjectLockConfigHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketObjectLockConfigHandler")
	// TODO: 实现 GET Bucket Object Lock Configuration 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetBucketReplicationConfigHandler 处理 GET Bucket Replication Configuration 请求
func GetBucketReplicationConfigHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketReplicationConfigHandler")
	// TODO: 实现 GET Bucket Replication Configuration 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetBucketVersioningHandler 处理 GET Bucket Versioning 请求
func GetBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketVersioningHandler")
	// TODO: 实现 GET Bucket Versioning 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetBucketNotificationHandler 处理 GET Bucket Notification 请求
func GetBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketNotificationHandler")
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取x-amz-expected-bucket-owner头部
	expectedOwnerID := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwnerID = strings.TrimSpace(expectedOwnerID)

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	// 获取桶信息
	_bucket, err := bs.GetBucketInfo(&sb.BaseBucketParams{
		BucketName:  bucket,
		Location:    region,
		AccessKeyID: accessKeyID,
	})
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else {
			logger.GetLogger("boulder").Errorf("failed to get bucket info: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	if expectedOwnerID != "" && expectedOwnerID != _bucket.Owner.ID {
		logger.GetLogger("boulder").Errorf("bucket owner mismatch: expected %s, got %s", expectedOwnerID, _bucket.Owner.ID)
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
	}

	// 如果没有配置，返回空的通知配置
	if _bucket.Notification == nil {
		_bucket.Notification = &meta.EventNotificationConfiguration{}
	}

	// 设置XML命名空间
	_bucket.Notification.XMLName = xml.Name{Local: "NotificationConfiguration"}
	_bucket.Notification.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

	// 返回XML响应
	xhttp.WriteAWSSuc(w, r, _bucket.Notification)
}

// GetBucketACLHandler 处理 GET Bucket ACL 请求
func GetBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketACLHandler")

	// 获取请求变量
	bucket, _, _, accessKeyID := GetReqVar(r)

	// 获取x-amz-expected-bucket-owner头部
	expectedOwner := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwner = strings.TrimSpace(expectedOwner)

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 获取bucket信息
	_bucket, err := bs.GetBucketInfo(&sb.BaseBucketParams{
		BucketName:      bucket,
		AccessKeyID:     accessKeyID,
		ExpectedOwnerID: expectedOwner,
	})
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else {
			logger.GetLogger("boulder").Errorf("failed to get bucket info: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 准备返回的ACL信息

	if _bucket.ACL == nil {
		// 如果没有设置ACL，返回默认的ACL（所有者有FULL_CONTROL权限）
		_bucket.ACL = meta.NewAccessControlPolicy(meta.CanonicalUser{
			ID:          _bucket.Owner.ID,
			DisplayName: _bucket.Owner.DisplayName,
		})
		// 添加所有者的FULL_CONTROL权限
		_ = _bucket.ACL.AddGrant("CanonicalUser", _bucket.Owner.ID, _bucket.Owner.DisplayName, "", "", meta.PermissionFullControl)
	}

	// 设置XML命名空间
	_bucket.ACL.XMLName = xml.Name{Local: "AccessControlPolicy"}
	_bucket.ACL.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

	xhttp.WriteAWSSuc(w, r, _bucket.ACL)
}

// PutBucketACLHandler 处理 PUT Bucket ACL 请求
func PutBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketACLHandler")

	// 获取请求变量
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取x-amz-expected-bucket-owner头部
	expectedOwner := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwner = strings.TrimSpace(expectedOwner)

	// 获取Content-MD5头部并进行校验（如果需要）
	contentMD5 := r.Header.Get("Content-MD5")
	contentMD5 = strings.TrimSpace(contentMD5)
	// TODO: 实现Content-MD5校验逻辑

	// 获取x-amz-sdk-checksum-algorithm头部
	checksumAlgorithm := r.Header.Get("x-amz-sdk-checksum-algorithm")
	checksumAlgorithm = strings.TrimSpace(checksumAlgorithm)
	// TODO: 实现校验和算法处理逻辑

	// 获取并处理x-amz-acl头部
	predefinedACL := r.Header.Get(xhttp.AmzACL)
	predefinedACL = strings.TrimSpace(predefinedACL)

	// 读取请求体
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to read request body: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrBadRequest)
		return
	}

	// 初始化ACL配置
	var aclConfig meta.AccessControlPolicy
	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 获取当前桶信息，用于获取所有者信息
	bucketInfo, err := bs.GetBucketInfo(&sb.BaseBucketParams{
		BucketName:  bucket,
		Location:    region,
		AccessKeyID: accessKeyID,
	})
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			logger.GetLogger("boulder").Errorf("access denied for %s", accessKeyID)
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else {
			logger.GetLogger("boulder").Errorf("failed to get bucket info: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 设置所有者信息
	aclConfig = *meta.NewAccessControlPolicy(meta.CanonicalUser{
		ID:          bucketInfo.Owner.ID,
		DisplayName: bucketInfo.Owner.DisplayName,
	})

	// 处理x-amz-acl头部
	hasHeaderACL := false
	if predefinedACL != "" {
		hasHeaderACL = true
		logger.GetLogger("boulder").Debugf("Processing x-amz-acl: %s", predefinedACL)
		switch predefinedACL {
		case "private":
			// 默认权限，仅保留所有者权限
		case "public-read":
			if err := aclConfig.GrantPublicRead(); err != nil {
				logger.GetLogger("boulder").Errorf("failed to set public-read ACL: %v", err)
				xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
				return
			}
		case "public-read-write":
			if err := aclConfig.GrantPublicReadWrite(); err != nil {
				logger.GetLogger("boulder").Errorf("failed to set public-read-write ACL: %v", err)
				xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
				return
			}
		case "authenticated-read":
			if err := aclConfig.AddGrant("Group", "", "", "", meta.AuthUsersGroup, meta.PermissionRead); err != nil {
				logger.GetLogger("boulder").Errorf("failed to set authenticated-read ACL: %v", err)
				xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
				return
			}
		case "bucket-owner-read":
			// 此处应该获取存储桶所有者信息
		case "bucket-owner-full-control":
			// 此处应该获取存储桶所有者信息并授予完全控制权限
		default:
			logger.GetLogger("boulder").Errorf("invalid x-amz-acl value: %s", predefinedACL)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}
	}

	// 处理x-amz-grant-*系列头部
	processGrantHeader := func(headerName, permission string) {
		if headerValue := r.Header.Get(headerName); headerValue != "" {
			hasHeaderACL = true
			logger.GetLogger("boulder").Debugf("Processing %s: %s", headerName, headerValue)
			// 完整解析授权信息
			// 支持格式：
			// 1. 单一CanonicalUser ID: "1234567890123456789012345678901234567890123456789012345678901234"
			// 2. 带属性的CanonicalUser: id="123456...",displayName="example-user"
			// 3. 邮箱格式: emailAddress="user@example.com"
			// 4. 组URI格式: uri="http://acs.amazonaws.com/groups/global/AllUsers"

			// 拆分多个授权项（用逗号分隔）
			grantParts := strings.Split(headerValue, ",")
			for _, part := range grantParts {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}

				// 解析授权信息
				var granteeType, id, displayName, email, uri string

				// 检查是否包含属性格式
				if strings.Contains(part, "=") {
					// 解析格式如: id="...",displayName="..." 或 emailAddress="..." 或 uri="..."
					attrs := strings.Split(part, ",")
					for _, attr := range attrs {
						parts := strings.SplitN(strings.TrimSpace(attr), "=", 2)
						if len(parts) != 2 {
							continue
						}
						key := strings.TrimSpace(parts[0])
						value := strings.Trim(parts[1], `"`)

						switch key {
						case "id":
							id = value
						case "displayName":
							displayName = value
						case "emailAddress":
							granteeType = "AmazonCustomerByEmail"
							email = value
						case "emailaddress":
							// 兼容小写形式
							granteeType = "AmazonCustomerByEmail"
							email = value
						case "uri":
							granteeType = "Group"
							uri = value
						}
					}
				} else {
					// 简化情况：直接将值作为CanonicalUser ID
					granteeType = "CanonicalUser"
					id = part
				}

				// 调用AddGrant添加授权
				if err := aclConfig.AddGrant(granteeType, id, displayName, email, uri, permission); err != nil {
					logger.GetLogger("boulder").Errorf("failed to process grant header %s: %v", headerName, err)
					xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
					return
				}
			}
		}
	}

	processGrantHeader("x-amz-grant-full-control", meta.PermissionFullControl)
	processGrantHeader("x-amz-grant-read", meta.PermissionRead)
	processGrantHeader("x-amz-grant-read-acp", meta.PermissionReadACP)
	processGrantHeader("x-amz-grant-write", meta.PermissionWrite)
	processGrantHeader("x-amz-grant-write-acp", meta.PermissionWriteACP)

	// 处理Content-MD5验证
	if contentMD5 != "" && len(body) > 0 {
		logger.GetLogger("boulder").Debugf("Processing Content-MD5 validation")
		hash := md5.Sum(body)
		computedMD5 := hex.EncodeToString(hash[:])
		if computedMD5 != contentMD5 {
			logger.GetLogger("boulder").Errorf("Content-MD5 mismatch: computed=%s, header=%s", computedMD5, contentMD5)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidDigest)
			return
		}
	}

	// 根据S3标准，头部定义的ACL优先级高于body中的ACL
	// 只有在头部没有定义ACL的情况下才处理请求体中的ACL
	if !hasHeaderACL && len(body) > 0 {
		logger.GetLogger("boulder").Debugf("Parsing ACL from XML request body")
		// 解析XML请求体
		if err := xml.Unmarshal(body, &aclConfig); err != nil {
			logger.GetLogger("boulder").Errorf("failed to unmarshal ACL configuration: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedXML)
			return
		}
	}

	if err := aclConfig.Validate(); err != nil {
		logger.GetLogger("boulder").Errorf("failed to validate ACL configuration: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedPolicy)
		return
	}

	// 调用service层方法设置ACL配置
	err = bs.PutBucketACL(&sb.BaseBucketParams{
		BucketName:      bucket,
		Location:        region,
		AccessKeyID:     accessKeyID,
		ExpectedOwnerID: expectedOwner,
	}, &aclConfig)

	// 处理错误
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else {
			logger.GetLogger("boulder").Errorf("failed to put bucket ACL: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 返回成功响应
	w.WriteHeader(http.StatusOK)
	logger.GetLogger("boulder").Tracef("successfully set ACL for bucket: %s", bucket)
}

// GetBucketCorsHandler 处理 GET Bucket CORS 请求 (Dummy)
func GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketCorsHandler")
	// TODO: 实现 GET Bucket CORS 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
}

// PutBucketCorsHandler 处理 PUT Bucket CORS 请求 (Dummy)
func PutBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketCorsHandler")
	// TODO: 实现 PUT Bucket CORS 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
}

// DeleteBucketCorsHandler 处理 DELETE Bucket CORS 请求 (Dummy)
func DeleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteBucketCorsHandler")
	// TODO: 实现 DELETE Bucket CORS 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
}

// GetBucketWebsiteHandler 处理 GET Bucket Website 请求 (Dummy)
func GetBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketWebsiteHandler")
	// TODO: 实现 GET Bucket Website 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
}

// GetBucketAccelerateHandler 处理 GET Bucket Accelerate 请求 (Dummy)
func GetBucketAccelerateHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketAccelerateHandler")
	// TODO: 实现 GET Bucket Accelerate 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
}

// GetBucketRequestPaymentHandler 处理 GET Bucket Request Payment 请求 (Dummy)
func GetBucketRequestPaymentHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketRequestPaymentHandler")
	// TODO: 实现 GET Bucket Request Payment 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
}

// GetBucketLoggingHandler 处理 GET Bucket Logging 请求 (Dummy)
func GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketLoggingHandler")
	// TODO: 实现 GET Bucket Logging 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
}

// GetBucketTaggingHandler 处理 GET Bucket Tagging 请求
func GetBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketTaggingHandler")
	bucket, _, region, accessKeyID := GetReqVar(r)

	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 获取桶的元数据
	_bucket, err := bs.GetBucketInfo(&sb.BaseBucketParams{
		BucketName:  bucket,
		Location:    region,
		AccessKeyID: accessKeyID,
	})

	if err != nil || _bucket == nil {
		logger.GetLogger("boulder").Errorf("bucket %s does not exist: %v", bucket, err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
			return
		}
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	// 检查x-amz-expected-bucket-owner头部
	expectedOwner := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwner = strings.TrimSpace(expectedOwner)
	if expectedOwner != "" {
		// 检查存储桶的实际所有者是否与请求的所有者匹配
		if _bucket.Owner.ID != expectedOwner {
			logger.GetLogger("boulder").Errorf("bucket owner mismatch: expected %s, got %s", expectedOwner, _bucket.Owner.ID)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketOwnerAWSAccountID)
			return
		}
	}

	// 创建符合S3标准的响应结构
	result := &meta.Tagging{
		XMLName: xml.Name{Local: "Tagging"},
		XMLNS:   "http://s3.amazonaws.com/doc/2006-03-01/",
		TagSet:  meta.TagSet{Tags: []meta.Tag{}},
	}

	// 如果桶有标签，添加到响应中
	if _bucket.Tagging != nil && len(_bucket.Tagging.TagSet.Tags) > 0 {
		result.TagSet.Tags = _bucket.Tagging.TagSet.Tags
	}

	// 写入成功响应
	xhttp.WriteAWSSuc(w, r, result)
	logger.GetLogger("boulder").Tracef("successfully retrieved tagging for bucket: %s", bucket)
}

// DeleteBucketWebsiteHandler 处理 DELETE Bucket Website 请求
func DeleteBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteBucketWebsiteHandler")
	// TODO: 实现 DELETE Bucket Website 逻辑
	w.WriteHeader(http.StatusOK)
}

// DeleteBucketTaggingHandler 处理 DELETE Bucket Tagging 请求
func DeleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteBucketTaggingHandler")
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 获取x-amz-expected-bucket-owner头部
	expectedOwner := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwner = strings.TrimSpace(expectedOwner)

	// 调用PutBucketTagging方法，传入空的标签集合来删除所有标签
	err := bs.PutBucketTagging(&sb.BaseBucketParams{
		BucketName:      bucket,
		Location:        region,
		AccessKeyID:     accessKeyID,
		ExpectedOwnerID: expectedOwner,
		Tags:            []meta.Tag{},
	})

	// 处理错误
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
			return
		}

		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
			return
		}

		logger.GetLogger("boulder").Errorf("failed to delete bucket %s tagging: %v", bucket, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	// 返回成功响应（HTTP 204 No Content）
	w.WriteHeader(http.StatusNoContent)
	logger.GetLogger("boulder").Tracef("successfully deleted tagging for bucket: %s", bucket)
}

// GetBucketPolicyStatusHandler 处理 GET Bucket Policy Status 请求
func GetBucketPolicyStatusHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketPolicyStatusHandler")

	// 获取请求变量
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 获取x-amz-expected-bucket-owner头部
	expectedOwner := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwner = strings.TrimSpace(expectedOwner)

	// 获取桶信息
	_bucket, err := bs.GetBucketInfo(&sb.BaseBucketParams{
		BucketName:      bucket,
		Location:        region,
		AccessKeyID:     accessKeyID,
		ExpectedOwnerID: expectedOwner,
	})
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
			return
		}
		logger.GetLogger("boulder").Errorf("failed to get bucket info: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	isPublic := false
	if _bucket.Policy != nil {
		isPublic = _bucket.Policy.IsPolicyPublic()
	}

	// 构造策略状态响应
	policyStatus := sb.GetBucketPolicyStatusResult{
		XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/",
		PolicyStatus: sb.PolicyStatus{
			IsPublic: isPublic, // 使用正确的公开状态判断逻辑
		},
	}

	// 使用XML响应函数返回结果
	xhttp.WriteAWSSuc(w, r, policyStatus)
	logger.GetLogger("boulder").Tracef("successfully got policy status for bucket: %s", bucket)
}

// PutBucketLifecycleHandler 处理 PUT Bucket Lifecycle 请求
func PutBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketLifecycleHandler")

	// 获取请求变量
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 读取请求体
	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to read request body: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidRequest)
		return
	}
	defer r.Body.Close()

	// 解析XML请求体
	var lifecycleConfig meta.LifecycleConfiguration
	if err := xml.Unmarshal(body, &lifecycleConfig); err != nil {
		logger.GetLogger("boulder").Errorf("failed to unmarshal lifecycle configuration: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedXML)
		return
	}

	if len(lifecycleConfig.Rules) > 0 && len(body) > 0 {
		if err := lifecycleConfig.Validate(); err != nil {
			logger.GetLogger("boulder").Errorf("invalid bucket lifecycle: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidRequest)
			return
		}
	}

	// 获取x-amz-expected-bucket-owner头部
	expectedOwner := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwner = strings.TrimSpace(expectedOwner)

	// 调用service层方法设置生命周期配置
	err = bs.PutBucketLifecycle(&sb.BaseBucketParams{
		BucketName:      bucket,
		Location:        region,
		AccessKeyID:     accessKeyID,
		ExpectedOwnerID: expectedOwner,
	}, &lifecycleConfig)

	// 处理错误
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else {
			logger.GetLogger("boulder").Errorf("failed to put bucket lifecycle: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 返回成功响应
	w.WriteHeader(http.StatusOK)
	logger.GetLogger("boulder").Tracef("successfully set lifecycle configuration for bucket: %s", bucket)
}

// PutBucketReplicationConfigHandler 处理 PUT Bucket Replication Configuration 请求
func PutBucketReplicationConfigHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketReplicationConfigHandler")
	// TODO: 实现 PUT Bucket Replication Configuration 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutBucketEncryptionHandler 处理 PUT Bucket Encryption 请求
func PutBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketEncryptionHandler")
	// TODO: 实现 PUT Bucket Encryption 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutBucketPolicyHandler 处理 PUT Bucket Policy 请求
func PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketPolicyHandler")
	// 获取请求变量
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取x-amz-expected-bucket-owner头部
	expectedOwner := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwner = strings.TrimSpace(expectedOwner)

	// 读取请求体
	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to read request body: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrBadRequest)
		return
	}
	defer r.Body.Close()

	// 解析策略内容
	var policy meta.BucketPolicy
	if len(body) > 0 {
		// 策略内容是JSON格式的字符串
		if err := json.Unmarshal(body, &policy); err != nil {
			logger.GetLogger("boulder").Errorf("failed to parse bucket policy: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedPolicy)
			return
		}
		// 验证策略格式
		if err := policy.Validate(); err != nil {
			logger.GetLogger("boulder").Errorf("invalid bucket policy: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedPolicy)
			return
		}
	}

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	err = bs.PutBucketPolicy(&sb.BaseBucketParams{
		BucketName:      bucket,
		AccessKeyID:     accessKeyID,
		Location:        region,
		ExpectedOwnerID: expectedOwner,
	}, &policy)

	// 处理错误
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else {
			logger.GetLogger("boulder").Errorf("failed to put bucket policy: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 返回成功响应
	w.WriteHeader(http.StatusOK)
	logger.GetLogger("boulder").Infof("successfully set bucket policy for bucket: %s", bucket)
}

// PutBucketObjectLockConfigHandler 处理 PUT Bucket Object Lock Configuration 请求
func PutBucketObjectLockConfigHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketObjectLockConfigHandler")
	// TODO: 实现 PUT Bucket Object Lock Configuration 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutBucketTaggingHandler 处理 PUT Bucket Tagging 请求
func PutBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketTaggingHandler")
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 读取请求体
	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to read request body: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedRequestBody)
		return
	}
	defer r.Body.Close()

	// 解析请求体中的标签信息
	tagging := meta.Tagging{}
	if err := xml.Unmarshal(body, &tagging); err != nil {
		logger.GetLogger("boulder").Errorf("failed to unmarshal tagging XML: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedXML)
		return
	}

	// 验证标签数量限制（AWS S3 限制每个存储桶最多10个标签）
	if len(tagging.TagSet.Tags) > 10 {
		logger.GetLogger("boulder").Errorf("too many tags: %d", len(tagging.TagSet.Tags))
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidTag)
		return
	}

	if len(body) > 0 && len(tagging.TagSet.Tags) > 0 {
		err = tagging.Validate()
		if err != nil {
			logger.GetLogger("boulder").Errorf("invalid tagging XML: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidTag)
			return
		}
	}

	expectedOwner := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwner = strings.TrimSpace(expectedOwner)

	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	err = bs.PutBucketTagging(&sb.BaseBucketParams{
		BucketName:      bucket,
		Location:        region,
		AccessKeyID:     accessKeyID,
		ExpectedOwnerID: expectedOwner,
		Tags:            tagging.TagSet.Tags,
	})
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
			return
		}

		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
			return
		}

		logger.GetLogger("boulder").Errorf("failed to put bucket %s tagging: %v", bucket, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	// 返回成功响应
	w.WriteHeader(http.StatusOK)
	logger.GetLogger("boulder").Tracef("successfully set tagging for bucket: %s", bucket)
}

// PutBucketVersioningHandler 处理 PUT Bucket Versioning 请求
func PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketVersioningHandler")
	// TODO: 实现 PUT Bucket Versioning 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutBucketNotificationHandler 设置存储桶的通知配置
func PutBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketNotificationHandler")
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 获取请求体
	data, err := io.ReadAll(r.Body)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to read request body: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidRequest)
		return
	}
	defer r.Body.Close()

	// 解析XML
	notification := meta.EventNotificationConfiguration{}
	if len(data) > 0 {
		if err := xml.Unmarshal(data, &notification); err != nil {
			logger.GetLogger("boulder").Errorf("failed to unmarshal notification configuration: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedXML)
			return
		}
		if err := notification.Validate(); err != nil {
			logger.GetLogger("boulder").Errorf("invalid notification configuration: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrEventNotification)
			return
		}
	}

	// 获取x-amz-expected-bucket-owner头部
	expectedOwnerID := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwnerID = strings.TrimSpace(expectedOwnerID)

	err = bs.PutBucketNotification(&sb.BaseBucketParams{
		BucketName:      bucket,
		AccessKeyID:     accessKeyID,
		Location:        region,
		ExpectedOwnerID: expectedOwnerID,
	}, &notification)

	// 调用服务层方法
	if err != nil {
		// 处理错误
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else {
			logger.GetLogger("boulder").Errorf("failed to set bucket notification configuration: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 返回200 OK
	w.WriteHeader(http.StatusOK)
}

// PutBucketHandler 处理 PUT Bucket (CreateBucket) 请求
func PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Tracef("API called: PutBucketHandler")
	bucket, _, _, accessKeyID := GetReqVar(r)

	objectLockEnabled := false
	if vs := r.Header.Get(xhttp.AmzObjectLockEnabled); len(vs) > 0 {
		v := strings.ToLower(vs)
		switch v {
		case "true", "false":
			objectLockEnabled = v == "true"
		default:
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidRequest)
			return
		}
	}
	logger.GetLogger("boulder").Tracef("create bucket %s get object lock option %v", bucket, objectLockEnabled)

	var locationConstraint sb.CreateBucketLocationConfiguration
	err := utils.XmlDecoder(r.Body, &locationConstraint, r.ContentLength)
	if err == nil {
		logger.GetLogger("boulder").Tracef("creating bucket location configuration %+v", locationConstraint)
	}

	if err := utils.CheckValidBucketName(bucket); err != nil {
		logger.GetLogger("boulder").Errorf("check bucket name %s invalid %v", bucket, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketName)
		return
	}

	// 校验有没有 创建通的权限 policy.CreateBucketAction
	// 每个账户创建的桶数量有上限，检查有没有超限

	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	err = bs.CreateBucket(&sb.BaseBucketParams{
		BucketName:        bucket,
		Location:          locationConstraint.Location,
		ObjectLockEnabled: objectLockEnabled,
		AccessKeyID:       accessKeyID,
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed create bucket err: %v", err)

		if errors.Is(err, xhttp.ToError(xhttp.ErrBucketAlreadyOwnedByYou)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrBucketAlreadyOwnedByYou)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrBucketAlreadyExists)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrBucketAlreadyExists)
		} else {
			xhttp.WriteAWSErr(w, r, xhttp.ErrBucketMetadataNotInitialized)
		}
		return
	}
	logger.GetLogger("boulder").Tracef("success bucket created: %v", bucket)
	// Make sure to add Location information here only for bucket
	w.Header().Set(xhttp.Location, "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

// HeadBucketHandler 检查存储桶是否存在
func HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Tracef("API called: HeadBucketHandler")
	bucket, _, region, accessKeyID := GetReqVar(r)

	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	_bucket, err := bs.GetBucketInfo(&sb.BaseBucketParams{
		BucketName:  bucket,
		AccessKeyID: accessKeyID,
	})
	if err != nil || _bucket == nil {
		logger.GetLogger("boulder").Errorf("bucket %s does not exist: %v", bucket, err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
			return
		}
		xhttp.WriteAWSErr(w, r, xhttp.ErrBucketMetadataNotInitialized)
	}
	w.Header().Set(xhttp.Location, "/"+bucket)
	w.Header().Set(xhttp.AmzBucketRegion, region)
	w.WriteHeader(http.StatusOK)
}

// DeleteBucketPolicyHandler 处理 DELETE Bucket Policy 请求
func DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteBucketPolicyHandler")

	// 获取请求变量
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取 x-amz-expected-bucket-owner 头部
	expectedOwnerID := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwnerID = strings.TrimSpace(expectedOwnerID)

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 调用服务层方法删除存储桶策略
	err := bs.PutBucketPolicy(&sb.BaseBucketParams{
		BucketName:      bucket,
		AccessKeyID:     accessKeyID,
		Location:        region,
		ExpectedOwnerID: expectedOwnerID,
	}, nil)

	// 处理错误
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else {
			logger.GetLogger("boulder").Errorf("failed to delete bucket policy: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 返回成功响应
	logger.GetLogger("boulder").Infof("successfully deleted bucket policy for bucket: %s", bucket)
	w.WriteHeader(http.StatusNoContent)
}

// DeleteBucketReplicationConfigHandler 处理 DELETE Bucket Replication Configuration Request
func DeleteBucketReplicationConfigHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteBucketReplicationConfigHandler")
	// TODO: 实现 DELETE Bucket Replication Configuration 逻辑
	w.WriteHeader(http.StatusOK)
}

// DeleteBucketLifecycleHandler 处理 DELETE Bucket Lifecycle Request
func DeleteBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteBucketLifecycleHandler")

	// 获取请求变量
	bucket, _, region, accessKeyID := GetReqVar(r)

	// 获取 x-amz-expected-bucket-owner 头部
	expectedOwnerID := r.Header.Get("x-amz-expected-bucket-owner")
	expectedOwnerID = strings.TrimSpace(expectedOwnerID)

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 调用服务层方法清除生命周期配置
	err := bs.PutBucketLifecycle(&sb.BaseBucketParams{
		BucketName:      bucket,
		AccessKeyID:     accessKeyID,
		Location:        region,
		ExpectedOwnerID: expectedOwnerID,
	}, nil)

	// 处理错误
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		} else {
			logger.GetLogger("boulder").Errorf("failed to put bucket lifecycle: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 返回成功响应
	w.WriteHeader(http.StatusNoContent)
}

// DeleteBucketEncryptionHandler 处理 DELETE Bucket Encryption Request
func DeleteBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteBucketEncryptionHandler")
	// TODO: 实现 DELETE Bucket Encryption 逻辑
	w.WriteHeader(http.StatusOK)
}

// DeleteBucketHandler 删除存储桶
func DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteBucketHandler")
	bucket, _, _, accessKeyID := GetReqVar(r)

	if err := utils.CheckValidBucketName(bucket); err != nil {
		logger.GetLogger("boulder").Errorf("DeleteBucketHandler: bucket name is empty")
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketName)
		return
	}

	// 获取bucket服务
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil: %v", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 执行删除操作
	err := bs.DeleteBucket(&sb.BaseBucketParams{
		BucketName:  bucket,
		AccessKeyID: accessKeyID,
	})
	// 根据不同的错误类型返回不同的错误响应
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrBucketNotEmpty)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrBucketNotEmpty)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete bucket %s: %v", bucket, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrBucketMetadataNotInitialized)
		return
	}

	logger.GetLogger("boulder").Tracef("successfully deleted bucket: %v", bucket)
	// 返回204 No Content表示成功删除
	w.WriteHeader(http.StatusNoContent)
}
