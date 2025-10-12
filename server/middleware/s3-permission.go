package middleware

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/utils"
	"github.com/mageg-x/dedups3/meta"
	"github.com/mageg-x/dedups3/service/bucket"
	"github.com/mageg-x/dedups3/service/iam"
	"github.com/mageg-x/dedups3/service/object"
)

// S3AuthorizationMiddleware 提供S3 API的通用鉴权中间件，按照S3标准综合评估权限
func S3AuthorizationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.GetLogger("dedups3").Debugf("s3 authorization check for %s %s", r.Method, r.URL.Path)

		// 从请求上下文获取访问密钥信息
		ctx := r.Context()
		accessKeyID, ok := ctx.Value("accesskey").(string)
		if !ok || accessKeyID == "" {
			logger.GetLogger("dedups3").Errorf("no access key in context")
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
			return
		}

		// 获取IAM服务
		iamService := iam.GetIamService()
		if iamService == nil {
			logger.GetLogger("dedups3").Errorf("failed to get iam service")
			xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
			return
		}

		// 获取访问密钥信息
		ak, err := iamService.GetAccessKey(accessKeyID)
		if ak == nil || err != nil {
			logger.GetLogger("dedups3").Errorf("get access key failed: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidAccessKeyID)
			return
		}

		if !ak.Status {
			logger.GetLogger("dedups3").Errorf("access key disable")
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessKeyDisabled)
			return
		}

		if ak.IsExpired() {
			logger.GetLogger("dedups3").Errorf("access key is expired")
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidAccessKeyID)
			return
		}

		// 从请求中获取bucket和object信息
		vars := utils.DecodeVars(mux.Vars(r))
		bucketName := vars["bucket"]
		objectKey := vars["object"]

		// 获取 S3 API操作名称
		var s3Action string
		route := mux.CurrentRoute(r)
		if route != nil {
			s3Action = route.GetName()
		}

		logger.GetLogger("dedups3").Debugf("s3 action: %s, bucket: %s, object: %s", s3Action, bucketName, objectKey)
		if s3Action == "" {
			logger.GetLogger("dedups3").Errorf("s3 action not found")
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}

		// 鉴权
		allow, errCode := IsAllowed(accessKeyID, ak.AccountID, ak.Username, bucketName, objectKey, s3Action)
		if errCode != xhttp.ErrNone || !allow {
			logger.GetLogger("dedups3").Errorf("evaluate permission failed: %v", xhttp.ToError(errCode))
			xhttp.WriteAWSErr(w, r, errCode)
			return
		}

		// 鉴权通过，继续处理请求
		next.ServeHTTP(w, r)
	})
}

// 1、来自资源策略（Bucket Policy和ACL）的显式拒绝
// 2、来自用户策略（IAM策略）的显式拒绝
// 3、来自资源策略的显式允许
// 4、来自用户策略的显式允许
// 5、默认拒绝
// IsAllowed 实现S3权限评估逻辑，根据S3标准综合评估用户是否有权限执行特定操作
func IsAllowed(accessKeyID, accountID, userName, bucketName, objKey, s3Action string) (bool, xhttp.APIErrorCode) {
	logger.GetLogger("dedups3").Debugf("isallowed: action=%s, bucket=%s, object=%s, accountid=%s",
		s3Action, bucketName, objKey, accountID)

	iamService := iam.GetIamService()
	if iamService == nil {
		logger.GetLogger("dedups3").Errorf("failed to get iam service")
		return false, xhttp.ErrInternalError
	}

	ac, err := iamService.GetAccount(accountID)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account info: %v", err)
		return false, xhttp.ErrInternalError
	}

	resourceARN := meta.BuildResourceARN(bucketName, objKey)

	// 1. 账户级别操作特殊处理
	if s3Action == "s3:ListBuckets" || s3Action == "s3:CreateBucket" {
		// 这些操作不依赖于特定bucket，只检查IAM策略
		if iamService.IsAllow(accountID, userName, s3Action, "*") {
			return true, xhttp.ErrNone
		}
		return false, xhttp.ErrAccessDenied
	}

	// 2. 对于需要bucket的操作，bucket名不能为空
	if bucketName == "" {
		logger.GetLogger("dedups3").Errorf("bucket name is required for action: %s", s3Action)
		return false, xhttp.ErrInvalidArgument
	}

	// 3. 获取桶信息
	bs := bucket.GetBucketService()
	if bs == nil {
		logger.GetLogger("dedups3").Errorf("failed to get bucket service")
		return false, xhttp.ErrInternalError
	}

	bucketInfo, err := bs.GetBucketInfo(&bucket.BaseBucketParams{
		BucketName:  bucketName,
		AccessKeyID: accessKeyID,
	})
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			logger.GetLogger("dedups3").Debugf("bucket not found during permission check: %s", bucketName)
			return false, xhttp.ErrNoSuchBucket
		}
		logger.GetLogger("dedups3").Errorf("failed to get bucket info: %v", err)
		return false, xhttp.ErrInternalError
	}

	// 4、获取所需的ACL权限
	requiredPermission := getRequiredACLPermission(s3Action)
	currentUser := meta.Grantee{
		Type: "CanonicalUser",
		ID:   accountID,
	}

	// 5、根据S3标准，桶所有者对桶和其中的对象有完全控制权
	if bucketInfo.Owner.ID == accountID && iamService.IsRootUser(accountID, userName) {
		logger.GetLogger("dedups3").Debugf("access allowed: user %s is bucket owner", userName)
		return true, xhttp.ErrNone
	}

	// 6. 检查IAM策略（同一账户内，IAM策略优先于基于资源的策略）
	if bucketInfo.Owner.ID == accountID && iamService.IsAllow(accountID, userName, s3Action, resourceARN) {
		logger.GetLogger("dedups3").Debugf("allowed by IAM policy")
		return true, xhttp.ErrNone
	}

	// 7. 检查桶策略
	if bucketInfo.Policy != nil {
		arnList := make([]string, 0)
		userARN := meta.FormatUserARN(accountID, userName)
		arnList = append(arnList, userARN)
		if user, err := iamService.GetUser(accountID, userName, userName); err == nil && user != nil {
			// 添加用户所属组的ARN
			for groupName, _ := range user.Groups {
				groupARN := meta.FormatGroupARN(ac.AccountID, groupName)
				arnList = append(arnList, groupARN)
			}
			// 添加用户所属角色的ARN
			for roleName, _ := range user.Roles {
				roleARN := meta.FormatRoleARN(ac.AccountID, roleName)
				arnList = append(arnList, roleARN)
			}
		}

		for _, arn := range arnList {
			allowedByPolicy, err := bucketInfo.Policy.IsAllowed(arn, s3Action, resourceARN, nil)
			if err != nil {
				logger.GetLogger("dedups3").Errorf("error evaluating bucket policy: %v", err)
			} else {
				if allowedByPolicy {
					logger.GetLogger("dedups3").Debugf("allowed by bucket policy")
					return true, xhttp.ErrNone
				}
			}
		}
	}

	// 8. 检查桶ACL
	if bucketInfo.ACL != nil {
		if bucketInfo.ACL.HasPermission(currentUser, requiredPermission) {
			logger.GetLogger("dedups3").Debugf("allowed by bucket ACL: %s", requiredPermission)
			return true, xhttp.ErrNone
		}
	}

	// 9. 对于对象操作，检查对象ACL
	if objKey != "" && !isListBucketAction(s3Action) {
		allowedByObjectACL, err := checkObjectACL(bucketName, objKey, accessKeyID, currentUser, requiredPermission)
		if err != nil {
			logger.GetLogger("dedups3").Debugf("object ACL check error: %v", err)
		} else if allowedByObjectACL {
			logger.GetLogger("dedups3").Debugf("allowed by object ACL: %s", requiredPermission)
			return true, xhttp.ErrNone
		}
	}

	// 10. 默认拒绝
	logger.GetLogger("dedups3").Debugf("access denied by ACL checks")
	return false, xhttp.ErrAccessDenied
}

// isListBucketAction 检查是否是列表桶操作
func isListBucketAction(s3Action string) bool {
	return s3Action == "s3:ListBucket" || s3Action == "s3:ListBucketVersions"
}

// getRequiredACLPermission 根据S3操作获取所需的ACL权限
func getRequiredACLPermission(s3Action string) string {
	switch {
	case strings.HasPrefix(s3Action, "s3:Get") || isListBucketAction(s3Action):
		return meta.PermissionRead
	case strings.HasPrefix(s3Action, "s3:Put") || strings.HasPrefix(s3Action, "s3:Delete"):
		return meta.PermissionWrite
	case s3Action == "s3:GetBucketAcl" || s3Action == "s3:GetObjectAcl":
		return meta.PermissionReadACP
	case s3Action == "s3:PutBucketAcl" || s3Action == "s3:PutObjectAcl":
		return meta.PermissionWriteACP
	default:
		return meta.PermissionFullControl
	}
}

// checkObjectACL 检查对象ACL权限
func checkObjectACL(bucketName, objKey, accessKeyID string, currentUser meta.Grantee, requiredPermission string) (bool, error) {
	os := object.GetObjectService()
	if os == nil {
		return false, errors.New("object service not available")
	}

	obj, err := os.HeadObject(&object.BaseObjectParams{
		BucketName:  bucketName,
		ObjKey:      objKey,
		AccessKeyID: accessKeyID,
	})

	if err != nil {
		// 对于创建操作，对象不存在是正常的
		if strings.Contains(err.Error(), "not exist") || strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}

	if obj.ACL != nil {
		return obj.ACL.HasPermission(currentUser, requiredPermission), nil
	}

	return false, nil
}
