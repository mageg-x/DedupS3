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
	"net/http"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gorilla/mux"

	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/utils"
	sb "github.com/mageg-x/boulder/service/bucket"
)

type ListAllMyBucketsResult struct {
	XMLName xml.Name       `xml:"ListAllMyBucketsResult"`
	Owner   types.Owner    `xml:"Owner"`
	Buckets []types.Bucket `xml:"Buckets>Bucket"`
}

// GetReqVar 获取请求中一些通用的变量
func GetReqVar(r *http.Request) (string, string, string, string) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]
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

	buckets, owner, err := bs.ListBuckets(sb.BaseBucketParams{
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
	// TODO: 实现 GET Bucket Location 逻辑
	w.WriteHeader(http.StatusOK)
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
	// TODO: 实现 GET Bucket Tagging 逻辑
	w.WriteHeader(http.StatusOK)
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
	// TODO: 实现 DELETE Bucket Tagging 逻辑
	w.WriteHeader(http.StatusOK)
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
	// TODO: 实现 PUT Bucket Tagging 逻辑
	w.WriteHeader(http.StatusOK)
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
	err = bs.CreateBucket(sb.BaseBucketParams{
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
	_bucket, err := bs.GetBucketInfo(sb.BaseBucketParams{
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
	// TODO: 实现 DELETE Bucket 逻辑
	w.WriteHeader(http.StatusOK)
}
