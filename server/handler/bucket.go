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
	bucket, _, _, accessKeyID := GetReqVar(r)

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
	// TODO: 实现 GET Bucket Policy 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetBucketLifecycleHandler 处理 GET Bucket Lifecycle 请求
func GetBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketLifecycleHandler")
	// TODO: 实现 GET Bucket Lifecycle 逻辑
	w.WriteHeader(http.StatusOK)
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
	// TODO: 实现 GET Bucket Notification 逻辑
	w.WriteHeader(http.StatusOK)
}

// ListenNotificationHandler 处理 Listen Notification 请求
func ListenNotificationHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: ListenNotificationHandler")
	// TODO: 实现 Listen Notification 逻辑
	w.WriteHeader(http.StatusOK)
}

// ResetBucketReplicationStatusHandler 处理 Reset Bucket Replication Status 请求 (MinIO extension)
func ResetBucketReplicationStatusHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: ResetBucketReplicationStatusHandler")
	// TODO: 实现 Reset Bucket Replication Status 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetBucketACLHandler 处理 GET Bucket ACL 请求 (Dummy)
func GetBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetBucketACLHandler")
	// TODO: 实现 GET Bucket ACL 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
}

// PutBucketACLHandler 处理 PUT Bucket ACL 请求 (Dummy)
func PutBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketACLHandler")
	// TODO: 实现 PUT Bucket ACL 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
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
	// TODO: 实现 GET Bucket Policy Status 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutBucketLifecycleHandler 处理 PUT Bucket Lifecycle 请求
func PutBucketLifecycleHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketLifecycleHandler")
	// TODO: 实现 PUT Bucket Lifecycle 逻辑
	w.WriteHeader(http.StatusOK)
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
	// TODO: 实现 PUT Bucket Policy 逻辑
	w.WriteHeader(http.StatusOK)
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
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to read request body: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedRequestBody)
		return
	}
	defer r.Body.Close()

	// 解析请求体中的标签信息
	tagging := &meta.Tagging{}
	if err := xml.Unmarshal(bodyBytes, &tagging); err != nil {
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

	// 验证每个标签的键和值
	for i, tag := range tagging.TagSet.Tags {
		if tag.Key == "" {
			logger.GetLogger("boulder").Errorf("tag key cannot be empty at index %d", i)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidTag)
			return
		}
		if len(tag.Key) > 128 {
			logger.GetLogger("boulder").Errorf("tag key too long: %s", tag.Key)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidTag)
			return
		}
		if len(tag.Value) > 256 {
			logger.GetLogger("boulder").Errorf("tag value too long: %s", tag.Value)
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

// PutBucketNotificationHandler 处理 PUT Bucket Notification 请求
func PutBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutBucketNotificationHandler")
	// TODO: 实现 PUT Bucket Notification 逻辑
	w.WriteHeader(http.StatusOK)
}

// ResetBucketReplicationStartHandler 处理 Reset Bucket Replication Start 请求 (MinIO extension)
func ResetBucketReplicationStartHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: ResetBucketReplicationStartHandler")
	// TODO: 实现 Reset Bucket Replication Start 逻辑
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

// PostPolicyBucketHandler 处理 POST Policy Bucket 请求 (e.g., browser uploads)
func PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PostPolicyBucketHandler")
	// TODO: 实现 POST Policy Bucket 逻辑
	// Note: isRequestPostPolicySignatureV4 check should ideally be done before calling this handler
	// or the logic should be integrated here.
	w.WriteHeader(http.StatusOK)
}

// DeleteBucketPolicyHandler 处理 DELETE Bucket Policy 请求
func DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteBucketPolicyHandler")
	// TODO: 实现 DELETE Bucket Policy 逻辑
	w.WriteHeader(http.StatusOK)
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
	// TODO: 实现 DELETE Bucket Lifecycle 逻辑
	w.WriteHeader(http.StatusOK)
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
