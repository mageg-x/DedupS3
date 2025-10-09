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
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/mageg-x/dedups3/internal/aws"
	"github.com/mageg-x/dedups3/meta"

	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/utils"
	"github.com/mageg-x/dedups3/service/object"

	"github.com/mageg-x/dedups3/internal/logger"
)

func HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: HeadObjectHandler")
	//logger.GetLogger("dedups3").Infof("head obect header %#v", r.Header)
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	Range := r.Header.Get("Range")
	ifMatch := r.Header.Get(xhttp.IfMatch)
	ifMatch = strings.Trim(ifMatch, "\"")
	ifnoneMatch := r.Header.Get(xhttp.IfNoneMatch)
	ifnoneMatch = strings.Trim(ifnoneMatch, "\"")
	ifmodifiedSince := r.Header.Get(xhttp.IfModifiedSince)

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	objInfo, err := _os.HeadObject(&object.BaseObjectParams{
		BucketName:      bucket,
		ObjKey:          objectKey,
		AccessKeyID:     accessKeyID,
		IfMatch:         ifMatch,
		IfNoneMatch:     ifnoneMatch,
		IfModifiedSince: ifmodifiedSince,
	})
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
			return
		}
		logger.GetLogger("dedups3").Errorf("object %s not found err: %v", objectKey, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	if objInfo == nil {
		logger.GetLogger("dedups3").Errorf("object %s not found", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}

	logger.GetLogger("dedups3").Debugf("headObject object %#v", objInfo)

	// If-Match
	if ifMatch != "" && string(objInfo.ETag) != ifMatch {
		logger.GetLogger("dedups3").Errorf("Object %s is not matched with If-Match ETag %s:%s", objectKey, ifMatch, objInfo.ETag)
		xhttp.WriteAWSErr(w, r, xhttp.ErrPreconditionFailed)
		return
	}

	// If-None-Match
	if ifnoneMatch != "" && string(objInfo.ETag) != ifnoneMatch {
		logger.GetLogger("dedups3").Errorf("object %s is not matched with If-None-Match ETag %s:%s", objectKey, ifnoneMatch, objInfo.ETag)
		xhttp.WriteAWSErr(w, r, xhttp.ERRNotModify)
		return
	}

	// If-Modified-Since
	if ifmodifiedSince != "" {
		if since, err := http.ParseTime(ifmodifiedSince); err == nil {
			if !objInfo.LastModified.After(since) {
				logger.GetLogger("dedups3").Errorf("object %s is not last modified since %s", objectKey, since)
				xhttp.WriteAWSErr(w, r, xhttp.ERRNotModify)
				return
			}
		}
	}

	// 设置响应头
	w.Header().Set(xhttp.ContentType, objInfo.ContentType)
	w.Header().Set(xhttp.ContentLength, strconv.FormatInt(objInfo.Size, 10))
	w.Header().Set(xhttp.ETag, fmt.Sprintf("\"%s\"", objInfo.ETag))
	w.Header().Set(xhttp.LastModified, objInfo.LastModified.Format(http.TimeFormat))

	if objInfo.ContentEncoding != "" {
		w.Header().Set(xhttp.ContentEncoding, objInfo.ContentEncoding)
	}
	if objInfo.ContentLanguage != "" {
		w.Header().Set(xhttp.ContentLanguage, objInfo.ContentLanguage)
	}
	if objInfo.ContentDisposition != "" {
		w.Header().Set(xhttp.ContentDisposition, objInfo.ContentDisposition)
	}
	if objInfo.CacheControl != "" {
		w.Header().Set(xhttp.CacheControl, objInfo.CacheControl)
	}

	// 不支持head range
	if Range != "" {
		w.Header().Set(xhttp.AcceptRanges, "none")
	}

	// 设置用户元数据
	for key, value := range objInfo.UserMetadata {
		w.Header().Set(fmt.Sprintf("%s%s", xhttp.AMZMetPrefix, key), value)
	}

	w.WriteHeader(http.StatusOK)
}

// GetObjectAttributesHandler 处理 GET Object Attributes 请求
func GetObjectAttributesHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: GetObjectAttributesHandler")

	w.WriteHeader(http.StatusOK)
}

// GetObjectACLHandler 处理 GET Object ACL 请求
func GetObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: GetObjectACLHandler")

	// 获取请求参数
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	// 获取对象服务
	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 获取对象信息
	params := &object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
	}

	obj, err := _os.HeadObject(params)
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
			logger.GetLogger("dedups3").Errorf("object %s/%s does not exist", bucket, objectKey)
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			logger.GetLogger("dedups3").Errorf("access denied for %s", accessKeyID)
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else {
			logger.GetLogger("dedups3").Errorf("failed to get object: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 检查对象是否有ACL，如果没有则创建默认ACL
	var acl *meta.AccessControlPolicy
	if obj.ACL != nil {
		acl = obj.ACL
	} else {
		acl = meta.NewAccessControlPolicy(meta.CanonicalUser{
			ID:          obj.Owner.ID,
			DisplayName: obj.Owner.DisplayName,
		})
	}

	xhttp.WriteAWSSuc(w, r, acl)
}

// PutObjectACLHandler 处理 PUT Object ACL 请求
func PutObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: PutObjectACLHandler")

	// 获取请求参数
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	// 获取对象服务
	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	obj, err := _os.HeadObject(&object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
	})

	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
			logger.GetLogger("dedups3").Errorf("object %s/%s does not exist", bucket, objectKey)
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			logger.GetLogger("dedups3").Errorf("access denied for %s", accessKeyID)
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else {
			logger.GetLogger("dedups3").Errorf("failed to get object: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	acp := meta.NewAccessControlPolicy(meta.CanonicalUser{
		ID:          obj.Owner.ID,
		DisplayName: obj.Owner.DisplayName,
	})
	// 处理x-amz-acl头部
	if aclHeader := r.Header.Get(xhttp.AmzACL); aclHeader != "" {
		logger.GetLogger("dedups3").Debugf("Processing x-amz-acl: %s", aclHeader)
		switch aclHeader {
		case "private":
			// 默认权限，仅保留所有者权限
		case "public-read":
			if err := acp.GrantPublicRead(); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to set public-read ACL: %v", err)
				xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
				return
			}
		case "public-read-write":
			if err := acp.GrantPublicReadWrite(); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to set public-read-write ACL: %v", err)
				xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
				return
			}
		case "authenticated-read":
			if err := acp.AddGrant("Group", "", "", "", meta.AuthUsersGroup, meta.PermissionRead); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to set authenticated-read ACL: %v", err)
				xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
				return
			}
		case "bucket-owner-read":
			// 此处应该获取存储桶所有者信息
		case "bucket-owner-full-control":
			// 此处应该获取存储桶所有者信息并授予完全控制权限
		default:
			logger.GetLogger("dedups3").Errorf("invalid x-amz-acl value: %s", aclHeader)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}
	}

	// 处理x-amz-grant-*系列头部
	processGrantHeader := func(headerName, permission string) {
		if headerValue := r.Header.Get(headerName); headerValue != "" {
			logger.GetLogger("dedups3").Debugf("Processing %s: %s", headerName, headerValue)
			// 完整解析授权信息
			// 支持格式：
			// 1. 单一CanonicalUser ID: "1234567890123456789012345678901234567890123456789012345678901234"
			// 2. 带属性的CanonicalUser: id="123456...",displayName="example-user"
			// 3. 邮箱格式: emailAddress="user@example.com"
			// 4. 组URI格式: uri="http://acs.amazonaws.com/groups/global/AllUsers"

			// 解析授权信息
			granteeType := "CanonicalUser"
			id := ""
			displayName := ""
			email := ""
			uri := ""

			// 检查是否包含属性格式
			if strings.Contains(headerValue, "=") {
				// 解析格式如: id="...",displayName="..." 或 emailAddress="..." 或 uri="..."
				attrs := strings.Split(headerValue, ",")
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
					case "uri":
						granteeType = "Group"
						uri = value
					}
				}
			} else {
				// 简化情况：直接将值作为CanonicalUser ID
				id = headerValue
			}

			// 调用AddGrant添加授权
			if err := acp.AddGrant(granteeType, id, displayName, email, uri, permission); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to process grant header %s: %v", headerName, err)
				xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
				return
			}
		}
	}

	processGrantHeader("x-amz-grant-full-control", meta.PermissionFullControl)
	processGrantHeader("x-amz-grant-read", meta.PermissionRead)
	processGrantHeader("x-amz-grant-read-acp", meta.PermissionReadACP)
	processGrantHeader("x-amz-grant-write", meta.PermissionWrite)
	processGrantHeader("x-amz-grant-write-acp", meta.PermissionWriteACP)

	// 读取请求体中的XML数据
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to read request body: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrBadRequest)
		return
	}

	// 如果有XML请求体，并且之前没有通过头部设置ACL，则解析XML
	if len(body) > 0 && len(acp.AccessControlList.Grants) == 0 {
		logger.GetLogger("dedups3").Debugf("Parsing ACL from XML request body")
		if err := acp.ParseXML(body); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to parse ACL XML: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedXML)
			return
		}
	}

	// 处理Content-MD5验证
	if contentMD5 := r.Header.Get(xhttp.ContentMD5); contentMD5 != "" && len(body) > 0 {
		logger.GetLogger("dedups3").Debugf("Processing Content-MD5 validation")
		hash := md5.Sum(body)
		computedMD5 := hex.EncodeToString(hash[:])
		if computedMD5 != contentMD5 {
			logger.GetLogger("dedups3").Errorf("Content-MD5 mismatch: computed=%s, header=%s", computedMD5, contentMD5)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidDigest)
			return
		}
	}

	obj, err = _os.PutObjectAcl(&object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
	}, acp)

	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
			logger.GetLogger("dedups3").Errorf("object %s/%s does not exist", bucket, objectKey)
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			logger.GetLogger("dedups3").Errorf("access denied for %s", accessKeyID)
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else {
			logger.GetLogger("dedups3").Errorf("failed to get object: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	// 返回成功响应
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Last-Modified", obj.LastModified.Format(http.TimeFormat))
}

// GetObjectTaggingHandler 处理 GET Object Tagging 请求
func GetObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: GetObjectTaggingHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	// 获取对象服务
	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	obj, err := _os.HeadObject(&object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
	})

	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
		return
	}
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get object: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	// 构建标签响应结构
	tagging := meta.Tagging{
		XMLName: xml.Name{Local: "Tagging"},
		XMLNS:   "http://s3.amazonaws.com/doc/2006-03-01/",
		TagSet:  meta.TagSet{},
	}
	tagging.TagSet.Tags = make([]meta.Tag, 0, len(obj.Tags))

	// 转换map为Tag数组
	for k, v := range obj.Tags {
		tagging.TagSet.Tags = append(tagging.TagSet.Tags, meta.Tag{
			Key:   k,
			Value: v,
		})
	}

	xhttp.WriteAWSSuc(w, r, tagging)
}

// PutObjectTaggingHandler 处理 PUT Object Tagging 请求
func PutObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: PutObjectTaggingHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	// 读取请求体
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to read request body: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedRequestBody)
		return
	}
	defer r.Body.Close()

	// 解析XML标签数据
	var tagging meta.Tagging
	if err := xml.Unmarshal(bodyBytes, &tagging); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to unmarshal tagging XML: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedXML)
		return
	}

	// 构建标签map
	tagsMap := make(map[string]string)
	for _, tag := range tagging.TagSet.Tags {
		// 验证标签键和值
		if len(tag.Key) == 0 || len(tag.Key) > 128 {
			logger.GetLogger("dedups3").Errorf("invalid tag key length: %d", len(tag.Key))
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}
		if len(tag.Value) == 0 || len(tag.Value) > 256 {
			logger.GetLogger("dedups3").Errorf("invalid tag value length: %d", len(tag.Value))
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}
		tagsMap[tag.Key] = tag.Value
	}

	// 获取对象服务
	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 调用服务更新对象标签
	obj, err := _os.PutObjectTagging(&object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
	}, tagsMap)
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
			return
		}
		logger.GetLogger("dedups3").Errorf("failed to put object tagging: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	// 设置响应头
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Last-Modified", obj.LastModified.Format(http.TimeFormat))
}

// DeleteObjectTaggingHandler 处理 DELETE Object Tagging 请求
func DeleteObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: DeleteObjectTaggingHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)

	// 验证对象名是否有效
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	// 获取对象服务
	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 调用对象服务删除标签（通过设置空标签map实现）
	_, err := _os.PutObjectTagging(&object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
	}, make(map[string]string))

	// 处理错误
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}

	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete object tags: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	// 成功响应
	w.WriteHeader(http.StatusNoContent)
}

// SelectObjectContentHandler 处理 SELECT Object Content 请求
func SelectObjectContentHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: SelectObjectContentHandler")
	// TODO: 实现 SELECT Object Content 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetObjectRetentionHandler 处理 GET Object Retention 请求
func GetObjectRetentionHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: GetObjectRetentionHandler")
	// TODO: 实现 GET Object Retention 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetObjectLegalHoldHandler 处理 GET Object Legal Hold 请求
func GetObjectLegalHoldHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: GetObjectLegalHoldHandler")
	// TODO: 实现 GET Object Legal Hold 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetObjectLambdaHandler 处理 GET Object with Lambda ARN 请求
func GetObjectLambdaHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: GetObjectLambdaHandler")
	// TODO: 实现 GET Object with Lambda ARN 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetObjectHandler 获取对象
func GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: GetObjectHandler")
	//logger.GetLogger("dedups3").Infof("head obect header %#v", r.Header)
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}
	// 解析 Range 头
	rangeHeadStr := r.Header.Get(xhttp.Range)
	rangeHead, err := xhttp.ParseRequestRangeSpec(rangeHeadStr)
	if err != nil && rangeHeadStr != "" {
		logger.GetLogger("dedups3").Errorf("invalid range header: %s, error: %v", rangeHeadStr, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidRange)
		return
	}

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	obj, reader, err := _os.GetObject(r.Body, r.Header, &object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
		Range:       rangeHead,
	})

	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
		return
	}
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to fetch object %s: %v", objectKey, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}
	// 只有成功才 defer
	if reader != nil {
		defer reader.Close()
	}

	w.Header().Set(xhttp.AcceptRanges, "bytes")
	w.Header().Set(xhttp.ContentType, obj.ContentType)
	w.Header().Set(xhttp.ETag, fmt.Sprintf("\"%s\"", obj.ETag))
	w.Header().Set(xhttp.LastModified, obj.LastModified.Format(http.TimeFormat))
	if obj.ContentEncoding != "" {
		w.Header().Set(xhttp.ContentEncoding, obj.ContentEncoding)
	}
	if obj.ContentLanguage != "" {
		w.Header().Set(xhttp.ContentLanguage, obj.ContentLanguage)
	}
	if obj.ContentDisposition != "" {
		w.Header().Set(xhttp.ContentDisposition, obj.ContentDisposition)
	}
	if obj.CacheControl != "" {
		w.Header().Set(xhttp.CacheControl, obj.CacheControl)
	}

	// 设置用户元数据
	for key, value := range obj.UserMetadata {
		w.Header().Set(fmt.Sprintf("%s%s", xhttp.AMZMetPrefix, key), value)
	}
	//处理 Range
	if rangeHead != nil {
		start, length, err := rangeHead.GetOffsetLength(obj.Size)
		if err != nil || length <= 0 {
			logger.GetLogger("dedups3").Errorf("invalid range head: %s, error: %v", rangeHeadStr, err)
			// 返回 416
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidRange)
			return
		}

		w.Header().Set(xhttp.ContentRange, fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, obj.Size))
		w.Header().Set(xhttp.ContentLength, strconv.FormatInt(length, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		// 全量返回
		w.Header().Set(xhttp.ContentLength, strconv.FormatInt(obj.Size, 10))
		w.WriteHeader(http.StatusOK)
	}

	// 流式输出：防止大文件 OOM
	_, err = io.Copy(w, reader)
	if err != nil {
		// 判断是否是 broken pipe
		errorMsg := strings.ToLower(err.Error())
		if strings.Contains(errorMsg, "broken pipe") || strings.Contains(errorMsg, "epipe") || strings.Contains(errorMsg, "connection reset by peer") {
			// 不算服务端错误，不用 error 级别
			logger.GetLogger("dedups3").Infof("client disconnected during download: %v", err)
		} else {
			logger.GetLogger("dedups3").Errorf("write response body failed: %v", err)
		}
		return
	}
}

// CopyObjectHandler 复制对象
func CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: CopyObjectHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	ifMatch := r.Header.Get(xhttp.IfMatch)
	ifMatch = strings.Trim(ifMatch, "\"")
	ifnoneMatch := r.Header.Get(xhttp.IfNoneMatch)
	ifnoneMatch = strings.Trim(ifnoneMatch, "\"")
	ifmodifiedSince := r.Header.Get(xhttp.IfModifiedSince)
	SourceIfMatch := r.Header.Get(xhttp.AmzCopySourceIfMatch)
	SourceIfNoneMatch := r.Header.Get(xhttp.AmzCopySourceIfNoneMatch)
	SourceIfModifiedSince := r.Header.Get(xhttp.AmzCopySourceIfModifiedSince)
	SourceIfUnmodifiedSince := r.Header.Get(xhttp.AmzCopySourceIfUnmodifiedSince)

	// 获取源桶 和对象
	cpSrcPath := r.Header.Get(xhttp.AmzCopySource)
	if u, err := url.Parse(cpSrcPath); err == nil {
		cpSrcPath = u.EscapedPath()
	}
	cpSrcPath = strings.TrimPrefix(cpSrcPath, "/")
	parts := strings.SplitN(cpSrcPath, "/", 2) // 只分割一次

	if len(parts) != 2 {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidCopySource)
		return
	}

	srcBucket, srcObject := parts[0], parts[1]
	srcObject = utils.TrimLeadingSlash(srcObject) // 确保 object 不以 / 开头

	if srcBucket == "" || srcObject == "" {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidCopySource)
		return
	}

	dstSc := r.Header.Get(xhttp.AmzStorageClass)

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	obj, err := _os.CopyObject(srcBucket, srcObject, &object.BaseObjectParams{
		BucketName:              bucket,
		ObjKey:                  objectKey,
		AccessKeyID:             accessKeyID,
		StorageClass:            dstSc,
		IfMatch:                 ifMatch,
		IfNoneMatch:             ifnoneMatch,
		IfModifiedSince:         ifmodifiedSince,
		SourceIfMatch:           SourceIfMatch,
		SourceIfNoneMatch:       SourceIfNoneMatch,
		SourceIfModifiedSince:   SourceIfModifiedSince,
		SourceIfUnmodifiedSince: SourceIfUnmodifiedSince,
	})
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidObjectName)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to copy object %s: %v", objectKey, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}
	type CopyObjResp struct {
		ETag         meta.Etag `xml:"ETag,omitempty"`
		LastModified string    `xml:"LastModified,omitempty"`
	}
	result := CopyObjResp{
		ETag:         obj.ETag,
		LastModified: obj.LastModified.Format("2006-01-02T15:04:05.000Z"),
	}
	xhttp.WriteAWSSuc(w, r, result)
}

// PutObjectRetentionHandler 处理 PUT Object Retention 请求
func PutObjectRetentionHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: PutObjectRetentionHandler")
	// TODO: 实现 PUT Object Retention 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutObjectLegalHoldHandler 处理 PUT Object Legal Hold 请求
func PutObjectLegalHoldHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: PutObjectLegalHoldHandler")
	// TODO: 实现 PUT Object Legal Hold 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutObjectExtractHandler 处理 PUT Object with auto-extract 请求
func PutObjectExtractHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: PutObjectExtractHandler")
	// TODO: 实现 PUT Object with auto-extract 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutObjectHandler 上传对象
func PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: PutObjectHandler")
	//logger.GetLogger("dedups3").Infof("putobect header %#v", r.Header)
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("Invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	ifMatch := r.Header.Get(xhttp.IfMatch)
	ifnoneMatch := r.Header.Get(xhttp.IfNoneMatch)
	ifmodifiedSince := r.Header.Get(xhttp.IfModifiedSince)

	// content-type
	ct := r.Header.Get(xhttp.ContentType)
	if ct == "" {
		ct = "application/octet-stream"
	}

	// content Md5
	contentMd5 := r.Header.Get(xhttp.ContentMD5)
	// 去掉前后的双引号
	contentMd5 = strings.Trim(contentMd5, "\"")

	// Validate storage class metadata if present
	sc := r.Header.Get(xhttp.AmzStorageClass)
	sc = strings.TrimSpace(sc)
	if sc != "" {
		if err := utils.CheckValidStorageClass(sc); err != nil {
			logger.GetLogger("dedups3").Errorf("Invalid storage class: %s", sc)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidStorageClass)
			return
		}
	}

	// 包装 body
	body, err := aws.NewReader(r)
	if err != nil {
		xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedRequestBody)
		return
	}
	defer body.Close()

	contentLenStr := r.Header.Get(xhttp.AmzDecodedContentLength)
	if contentLenStr == "" {
		contentLenStr = r.Header.Get(xhttp.ContentLength)
	}
	contentLength := r.ContentLength
	if contentLenStr != "" {
		contentLength, err = strconv.ParseInt(contentLenStr, 10, 64)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("Invalid X-Amz-Decoded-Content-Length: %s", contentLenStr)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidDigest) // 或 ErrMalformedRequestBody
			return
		}
	}
	if contentLength < 0 {
		logger.GetLogger("dedups3").Errorf("Negative content length: %d", contentLength)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidDigest)
		return
	}

	size := contentLength

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("Object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	obj, err := _os.PutObject(body, r.Header, &object.BaseObjectParams{
		BucketName:      bucket,
		ObjKey:          objectKey,
		ContentType:     ct,
		ContentMd5:      contentMd5,
		ContentLen:      size,
		AccessKeyID:     accessKeyID,
		StorageClass:    sc,
		IfMatch:         ifMatch,
		IfNoneMatch:     ifnoneMatch,
		IfModifiedSince: ifmodifiedSince,
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
		logger.GetLogger("dedups3").Errorf("Error putting object: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	w.Header().Set(xhttp.ETag, fmt.Sprintf("\"%s\"", obj.ETag))
	w.WriteHeader(http.StatusOK)
}

// DeleteObjectHandler 删除对象
func DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: DeleteObjectHandler")
	logger.GetLogger("dedups3").Infof("API called: PutObjectHandler")
	//logger.GetLogger("dedups3").Infof("putobect header %#v", r.Header)
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("Invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("Object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	err := _os.DeleteObject(&object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
	})
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DeleteMultipleObjectsHandler  批量删除多个对象 DeleteObjects
func DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Infof("API called: DeleteMultipleObjectsHandler")
	bucket, _, _, accessKeyID := GetReqVar(r)

	// 读取请求体
	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to read request body: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrBadRequest)
		return
	}
	defer r.Body.Close()

	var deleteReq object.DeleteObjectsRequest
	if err := xml.Unmarshal(body, &deleteReq); err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to decode DeleteObjects XML: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedXML)
		return
	}

	if err := utils.CheckValidBucketName(bucket); err != nil {
		logger.GetLogger("dedups3").Errorf("Invalid bucket name: %s", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketName)
	}

	// 获取对象服务
	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("Object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	result := object.DeleteObjectsResponse{
		XMLName: xml.Name{Local: "DeleteResult"},
		XMLNS:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Deleted: make([]object.DeletedObject, 0),
		Errors:  make([]object.DeletedObjectErrors, 0),
	}
	// 检查Quiet模式，默认为false
	quiet := false
	if deleteReq.Quiet != nil {
		quiet = *deleteReq.Quiet
	}
	// 遍历所有要删除的对象
	for _, obj := range deleteReq.Objects {
		// 执行删除操作
		err := _os.DeleteObject(&object.BaseObjectParams{
			BucketName:  bucket,
			ObjKey:      obj.Key,
			AccessKeyID: accessKeyID,
		})
		if err == nil {
			// 删除成功
			if !quiet {
				result.Deleted = append(result.Deleted, object.DeletedObject{Key: obj.Key})
			}
			logger.GetLogger("dedups3").Tracef("Successfully deleted object: %s", obj.Key)
		} else {
			if !quiet {
				// 确定错误类型
				errorCode := "InternalError"
				errorMessage := err.Error()

				if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
					errorCode = "AccessDenied"
					errorMessage = "Access Denied"
				} else if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
					errorCode = "NoSuchKey"
					errorMessage = "The specified key does not exist"
				}
				result.Errors = append(result.Errors, object.DeletedObjectErrors{
					Key:     &obj.Key,
					Code:    errorCode,
					Message: errorMessage,
				})
			}
		}
	}
	xhttp.WriteAWSSuc(w, r, result)
}

// PostRestoreObjectHandler 处理 POST Restore Object 请求
func RestoreObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: PostRestoreObjectHandler")
	// TODO: 实现 POST Restore Object 逻辑
	w.WriteHeader(http.StatusOK)
}

func ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Infof("API called: ListObjectsV1Handler header is %#v", r.Header)

	bucket, _, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidBucketName(bucket); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid bucket name: %s", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketName)
		return
	}
	query := utils.DecodeQuerys(r.URL.Query())
	maxkeys := 1000
	if query.Get("max-keys") != "" {
		var err error
		if maxkeys, err = strconv.Atoi(query.Get("max-keys")); err != nil {
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidMaxKeys)
			return
		}
	}

	prefix := query.Get("prefix")
	marker := query.Get("marker")
	delimiter := query.Get("delimiter")
	encodingType := query.Get("encoding-type")
	logger.GetLogger("dedups3").Infof("get query %#v", query)
	if prefix != "" {
		if err := utils.CheckValidObjectNamePrefix(prefix); err != nil {
			logger.GetLogger("dedups3").Errorf("invalid prefix: %s", prefix)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
			return
		}
	}

	if encodingType != "" {
		if !strings.EqualFold(encodingType, "url") {
			logger.GetLogger("dedups3").Errorf("invalid encoding-type: %s", encodingType)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidEncodingMethod)
			return
		}
		encodingType = "url" // ✅ 标准化
	}

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	objects, commonPrefixes, isTruncated, nextMarker, err := _os.ListObjects(bucket, accessKeyID, prefix, marker, delimiter, maxkeys)
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Error listing objects: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	resp := object.ListObjectsResponse{
		XMLName:        xml.Name{Local: "ListBucketResult"},
		XMLNS:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:           bucket,
		Prefix:         prefix,
		Marker:         marker,
		Delimiter:      delimiter,
		EncodingType:   encodingType,
		NextMarker:     nextMarker,
		IsTruncated:    isTruncated,
		Contents:       make([]object.ObjectContent, 0, len(objects)),
		CommonPrefixes: make([]object.CommonPrefix, 0, len(commonPrefixes)),
	}

	for _, o := range objects {
		content := object.ObjectContent{
			Key:          o.Key,
			LastModified: o.LastModified.UTC(), // S3 使用 UTC
			ETag:         o.ETag,
			Size:         o.Size,
			StorageClass: o.StorageClass,
		}
		content.Owner = &meta.Owner{
			ID:          o.Owner.ID,
			DisplayName: o.Owner.DisplayName,
		}
		resp.Contents = append(resp.Contents, content)
	}

	for _, cp := range commonPrefixes {
		resp.CommonPrefixes = append(resp.CommonPrefixes, object.CommonPrefix{
			Prefix: cp,
		})
	}

	// 如果 encoding-type=url，对所有字符串进行 URL 编码
	if encodingType == "url" {
		encode := func(s string) string {
			return url.QueryEscape(s)
		}

		if resp.Prefix != "" {
			encoded := encode(resp.Prefix)
			resp.Prefix = encoded
		}
		if resp.Marker != "" {
			encoded := encode(resp.Marker)
			resp.Marker = encoded
		}
		if resp.NextMarker != "" {
			encoded := encode(resp.NextMarker)
			resp.NextMarker = encoded
		}

		// 编码 Contents.Key
		for i := range resp.Contents {
			encoded := encode(resp.Contents[i].Key)
			resp.Contents[i].Key = encoded
		}

		for i := range resp.CommonPrefixes {
			encoded := encode(resp.CommonPrefixes[i].Prefix)
			resp.CommonPrefixes[i].Prefix = encoded
		}
	}

	xhttp.WriteAWSSuc(w, r, resp)
}

// ListObjectsV2MHandler 处理 List Objects V2 with metadata 请求
func ListObjectsV2MHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: ListObjectsV2MHandler")
	// TODO: 实现 List Objects V2 逻辑
	w.WriteHeader(http.StatusOK)
}

// ListObjectsV2Handler 处理 List Objects V2 请求
func ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Infof("API called: ListObjectsV2Handler")

	bucket, _, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidBucketName(bucket); err != nil {
		logger.GetLogger("dedups3").Errorf("invalid bucket name: %s", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketName)
		return
	}
	query := utils.DecodeQuerys(r.URL.Query())
	maxkeys := 1000
	if query.Get("max-keys") != "" {
		var err error
		if maxkeys, err = strconv.Atoi(query.Get("max-keys")); err != nil {
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidMaxKeys)
			return
		}
	}

	prefix := query.Get("prefix")
	continuationToken := query.Get("continuation-token")
	startAfter := query.Get("start-after")
	delimiter := query.Get("delimiter")
	encodingType := query.Get("encoding-type")
	logger.GetLogger("dedups3").Infof("get query %#v", query)
	if prefix != "" {
		if err := utils.CheckValidObjectNamePrefix(prefix); err != nil {
			logger.GetLogger("dedups3").Errorf("invalid prefix: %s", prefix)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
			return
		}
	}

	if encodingType != "" {
		if !strings.EqualFold(encodingType, "url") {
			logger.GetLogger("dedups3").Errorf("invalid encoding-type: %s", encodingType)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidEncodingMethod)
			return
		}
		encodingType = "url" // ✅ 标准化
	}

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	objects, commonPrefixes, isTruncated, nextToken, err := _os.ListObjectsV2(bucket, accessKeyID, prefix, continuationToken, startAfter, delimiter, maxkeys)
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Error listing objects: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	resp := &object.ListObjectsV2Response{
		XMLName:               xml.Name{Local: "ListBucketResult"},
		XMLNS:                 "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                  bucket,
		Prefix:                prefix,
		MaxKeys:               maxkeys,
		Delimiter:             delimiter,
		KeyCount:              len(objects) + len(commonPrefixes),
		IsTruncated:           isTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: nextToken,
		EncodingType:          encodingType,
		StartAfter:            startAfter,

		Contents:       make([]object.ObjectContent, 0, len(objects)),
		CommonPrefixes: make([]object.CommonPrefix, 0, len(commonPrefixes)),
	}

	for _, o := range objects {
		content := object.ObjectContent{
			Key:          o.Key,
			LastModified: o.LastModified.UTC(), // S3 使用 UTC
			ETag:         o.ETag,
			Size:         o.Size,
			StorageClass: o.StorageClass,
		}
		content.Owner = &meta.Owner{
			ID:          o.Owner.ID,
			DisplayName: o.Owner.DisplayName,
		}
		resp.Contents = append(resp.Contents, content)
	}

	for _, cp := range commonPrefixes {
		resp.CommonPrefixes = append(resp.CommonPrefixes, object.CommonPrefix{
			Prefix: cp,
		})

	}

	// 如果 encoding-type=url，对所有字符串进行 URL 编码
	if encodingType == "url" {
		encode := func(s string) string {
			return url.QueryEscape(s)
		}

		if resp.Prefix != "" {
			encoded := encode(resp.Prefix)
			resp.Prefix = encoded
		}
		if resp.StartAfter != "" {
			encoded := encode(resp.StartAfter)
			resp.StartAfter = encoded
		}

		// 编码 Contents.Key
		for i := range resp.Contents {
			encoded := encode(resp.Contents[i].Key)
			resp.Contents[i].Key = encoded
		}

		for i := range resp.CommonPrefixes {
			encoded := encode(resp.CommonPrefixes[i].Prefix)
			resp.CommonPrefixes[i].Prefix = encoded
		}
	}
	xhttp.WriteAWSSuc(w, r, resp)
}

// ListObjectVersionsMHandler 处理 List Object Versions with metadata 请求
func ListObjectVersionsMHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: ListObjectVersionsMHandler")
	// TODO: 实现 List Object Versions with metadata 逻辑
	w.WriteHeader(http.StatusOK)
}

// ListObjectVersionsHandler 处理 List Object Versions 请求
func ListObjectVersionsHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: ListObjectVersionsHandler")
	// TODO: 实现 List Object Versions 逻辑
	w.WriteHeader(http.StatusOK)
}

// RenameObjectHandler 处理 PUT Object rename 请求
func RenameObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: RenameObjectHandler")

	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("dedups3").Errorf("Invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	// 获取请求头中的参数
	renameSource := r.Header.Get(xhttp.AmzRenameSource)
	if renameSource == "" {
		logger.GetLogger("dedups3").Errorf("Missing %s header", xhttp.AmzRenameSource)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
		return
	}

	// 目标对象的条件
	ifMatch := r.Header.Get(xhttp.IfMatch)
	ifNoneMatch := r.Header.Get(xhttp.IfNoneMatch)
	ifModifiedSince := r.Header.Get(xhttp.IfModifiedSince)
	ifUnmodifiedSince := r.Header.Get(xhttp.IfUnmodifiedSince)

	// 源对象的条件
	sourceIfMatch := r.Header.Get(xhttp.AmzRenameSourceIfMatch)
	sourceIfNoneMatch := r.Header.Get(xhttp.AmzRenameSourceIfNoneMatch)
	sourceIfModifiedSince := r.Header.Get(xhttp.AmzRenameSourceIfModifiedSince)
	sourceIfUnmodifiedSince := r.Header.Get(xhttp.AmzRenameSourceIfUnmodifiedSince)

	// 客户端令牌
	clientToken := r.Header.Get(xhttp.AmzClientToken)

	logger.GetLogger("dedups3").Infof("Rename %s object from %s to %s", bucket, renameSource, objectKey)

	// 获取对象服务
	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("Object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	_, err := _os.RenameObject(&object.BaseObjectParams{
		BucketName:              bucket,
		AccessKeyID:             accessKeyID,
		ObjKey:                  objectKey,
		DestObjKey:              renameSource,
		IfMatch:                 ifMatch,
		IfNoneMatch:             ifNoneMatch,
		IfModifiedSince:         ifModifiedSince,
		IfUnmodifiedSince:       ifUnmodifiedSince,
		SourceIfMatch:           sourceIfMatch,
		SourceIfNoneMatch:       sourceIfNoneMatch,
		SourceIfUnmodifiedSince: sourceIfUnmodifiedSince,
		SourceIfModifiedSince:   sourceIfModifiedSince,
		ClientToken:             clientToken,
	})

	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
			logger.GetLogger("dedups3").Errorf("object %s/%s does not exist", bucket, objectKey)
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			logger.GetLogger("dedups3").Errorf("access denied for %s", accessKeyID)
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrPreconditionFailed)) {
			logger.GetLogger("dedups3").Errorf("object %s/%s condition failed", bucket, objectKey)
			xhttp.WriteAWSErr(w, r, xhttp.ErrPreconditionFailed)
		} else {
			logger.GetLogger("dedups3").Errorf("failed to get object: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}
