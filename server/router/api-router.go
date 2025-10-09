package router

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/mageg-x/dedups3/handler"
	"github.com/mageg-x/dedups3/internal/config"
	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/middleware"
)

func registerAPIRouter(mr *mux.Router) {
	// init api router
	ar := mr.PathPrefix("/").HeadersRegexp(xhttp.Authorization, "AWS4-HMAC-SHA256 Credential=.+").Subrouter()
	// 应用AWS4签名验证中间件
	ar.Use(middleware.AWS4SigningMiddleware)
	ar.Use(middleware.S3AuthorizationMiddleware)

	var routers []*mux.Router
	cfg := config.Get()
	for _, domain := range cfg.Server.Domains {
		routers = append(routers, ar.Host("{bucket:.+}."+domain).Subrouter())
	}
	routers = append(routers, ar.PathPrefix("/{bucket}").Subrouter())

	for _, router := range routers {
		// not implemented api
		// Register all rejected object APIs
		for _, r := range handler.RejectedObjAPIs {
			router.Methods(r.Methods...).Path(r.Path).
				HandlerFunc(handler.NotImplementedHandler).
				Queries(r.Queries...)
		}

		// Object operations
		// HeadObject
		router.Methods(http.MethodHead).Path("/{object:.+}").HandlerFunc(handler.HeadObjectHandler).Name("s3:HeadObject")

		// GetObjectAttributes
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectAttributesHandler).Queries("attributes", "").Name("s3:GetObjectAttributes")

		// CopyObjectPart
		router.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(handler.CopyObjectPartHandler).Queries("partNumber", "{partNumber:.*}", "uploadId", "{uploadId:.*}").Name("s3:UploadPartCopy")
		// PutObjectPart
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectPartHandler).Queries("partNumber", "{partNumber:.*}", "uploadId", "{uploadId:.*}").Name("s3:UploadPart")
		// ListObjectParts
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.ListObjectPartsHandler).Queries("uploadId", "{uploadId:.*}").Name("s3:ListParts")
		// CompleteMultipartUpload
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(handler.CompleteMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Name("s3:CompleteMultipartUpload")
		// NewMultipartUpload
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(handler.NewMultipartUploadHandler).Queries("uploads", "").Name("s3:CreateMultipartUpload")
		// AbortMultipartUpload
		router.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(handler.AbortMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Name("s3:AbortMultipartUpload")
		// GetObjectACL - this is a dummy call.
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectACLHandler).Queries("acl", "").Name("s3:GetObjectAcl")
		// PutObjectACL - this is a dummy call.
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectACLHandler).Queries("acl", "").Name("s3:PutObjectAcl")
		// GetObjectTagging
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectTaggingHandler).Queries("tagging", "").Name("s3:GetObjectTagging")
		// PutObjectTagging
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectTaggingHandler).Queries("tagging", "").Name("s3:PutObjectTagging")
		// DeleteObjectTagging
		router.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(handler.DeleteObjectTaggingHandler).Queries("tagging", "").Name("s3:DeleteObjectTagging")
		// SelectObjectContent
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(handler.SelectObjectContentHandler).Queries("select", "").Queries("select-type", "2").Name("s3:SelectObjectContent")
		// GetObjectRetention
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectRetentionHandler).Queries("retention", "").Name("s3:GetObjectRetention")
		// GetObjectLegalHold
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectLegalHoldHandler).Queries("legal-hold", "").Name("s3:GetObjectLegalHold")
		// GetObject with lambda ARNs
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectLambdaHandler).Queries("lambdaArn", "{lambdaArn:.*}").Name("s3:GetObjectWithLambda")
		// GetObject
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectHandler).Name("s3:GetObject")
		// CopyObject
		router.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(handler.CopyObjectHandler).Name("s3:CopyObject")
		// PutObjectRetention
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectRetentionHandler).Queries("retention", "").Name("s3:PutObjectRetention")
		// PutObjectLegalHold
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectLegalHoldHandler).Queries("legal-hold", "").Name("s3:PutObjectLegalHold")

		// PutObject with auto-extract support for zip
		router.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzSnowballExtract, "true").HandlerFunc(handler.PutObjectExtractHandler).Name("s3:PutObjectExtract")

		// AppendObject to be rejected
		router.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzWriteOffsetBytes, "").HandlerFunc(handler.ErrorResponseHandler).Name("s3:AppendObject") // Assuming errorResponseHandler is the final handler

		// RenameObject
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.RenameObjectHandler).Queries("renameObject", "").Name("s3:RenameObject")

		// PutObject
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectHandler).Name("s3:PutObject")

		// DeleteObject
		router.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(handler.DeleteObjectHandler).Name("s3:DeleteObject")

		// PostRestoreObject
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(handler.RestoreObjectHandler).Queries("restore", "").Name("s3:RestoreObject")

		// Bucket operations

		// GetBucketLocation
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketLocationHandler).Queries("location", "").Name("s3:GetBucketLocation")
		// GetBucketPolicy
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketPolicyHandler).Queries("policy", "").Name("s3:GetBucketPolicy")
		// GetBucketLifecycle
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketLifecycleHandler).Queries("lifecycle", "").Name("s3:GetBucketLifecycleConfiguration")
		// GetBucketEncryption
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketEncryptionHandler).Queries("encryption", "").Name("s3:GetBucketEncryption")
		// GetBucketObjectLockConfig
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketObjectLockConfigHandler).Queries("object-lock", "").Name("s3:GetObjectLockConfiguration")
		// GetBucketReplicationConfig
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketReplicationConfigHandler).Queries("replication", "").Name("s3:GetBucketReplication")
		// GetBucketVersioning
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketVersioningHandler).Queries("versioning", "").Name("s3:GetBucketVersioning")
		// GetBucketNotification
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketNotificationHandler).Queries("notification", "").Name("s3:GetBucketNotificationConfiguration")

		// Dummy Bucket Calls
		// GetBucketACL -- this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketACLHandler).Queries("acl", "").Name("s3:GetBucketAcl")
		// PutBucketACL -- this is a dummy call.
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketACLHandler).Queries("acl", "").Name("s3:PutBucketAcl")
		// GetBucketCors - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketCorsHandler).Queries("cors", "").Name("s3:GetBucketCors")
		// PutBucketCors - this is a dummy call.
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketCorsHandler).Queries("cors", "").Name("s3:PutBucketCors")
		// DeleteBucketCors - this is a dummy call.
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketCorsHandler).Queries("cors", "").Name("s3:DeleteBucketCors")
		// GetBucketWebsiteHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketWebsiteHandler).Queries("website", "").Name("s3:GetBucketWebsite")
		// GetBucketAccelerateHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketAccelerateHandler).Queries("accelerate", "").Name("s3:GetBucketAccelerateConfiguration")
		// GetBucketRequestPaymentHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketRequestPaymentHandler).Queries("requestPayment", "").Name("s3:GetBucketRequestPayment")
		// GetBucketLoggingHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketLoggingHandler).Queries("logging", "").Name("s3:GetBucketLogging")

		// GetBucketTaggingHandler
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketTaggingHandler).Queries("tagging", "").Name("s3:GetBucketTagging")
		// DeleteBucketWebsiteHandler
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketWebsiteHandler).Queries("website", "").Name("s3:DeleteBucketWebsite")
		// DeleteBucketTaggingHandler
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketTaggingHandler).Queries("tagging", "").Name("s3:DeleteBucketTagging")

		// ListMultipartUploads
		router.Methods(http.MethodGet).HandlerFunc(handler.ListMultipartUploadsHandler).Queries("uploads", "").Name("s3:ListMultipartUploads")
		// ListObjectsV2M
		router.Methods(http.MethodGet).HandlerFunc(handler.ListObjectsV2MHandler).Queries("list-type", "2", "metadata", "true").Name("s3:ListObjectsV2WithMetadata")
		// ListObjectsV2
		router.Methods(http.MethodGet).HandlerFunc(handler.ListObjectsV2Handler).Queries("list-type", "2").Name("s3:ListObjectsV2")
		// ListObjectVersions
		router.Methods(http.MethodGet).HandlerFunc(handler.ListObjectVersionsMHandler).Queries("versions", "", "metadata", "true").Name("s3:ListObjectVersionsWithMetadata")
		// ListObjectVersions
		router.Methods(http.MethodGet).HandlerFunc(handler.ListObjectVersionsHandler).Queries("versions", "").Name("s3:ListObjectVersions")
		// GetBucketPolicyStatus
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketPolicyStatusHandler).Queries("policyStatus", "").Name("s3:GetBucketPolicyStatus")
		// PutBucketLifecycle
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketLifecycleHandler).Queries("lifecycle", "").Name("s3:PutBucketLifecycleConfiguration")
		// PutBucketReplicationConfig
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketReplicationConfigHandler).Queries("replication", "").Name("s3:PutBucketReplication")
		// PutBucketEncryption
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketEncryptionHandler).Queries("encryption", "").Name("s3:PutBucketEncryption")

		// PutBucketPolicy
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketPolicyHandler).Queries("policy", "").Name("s3:PutBucketPolicy")

		// PutBucketObjectLockConfig
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketObjectLockConfigHandler).Queries("object-lock", "").Name("s3:PutObjectLockConfiguration")
		// PutBucketTaggingHandler
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketTaggingHandler).Queries("tagging", "").Name("s3:PutBucketTagging")
		// PutBucketVersioning
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketVersioningHandler).Queries("versioning", "").Name("s3:PutBucketVersioning")
		// PutBucketNotification
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketNotificationConfigurationHandler).Queries("notification", "").Name("s3:PutBucketNotificationConfiguration")

		// PutBucket
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketHandler).Name("s3:CreateBucket")
		// HeadBucket
		router.Methods(http.MethodHead).HandlerFunc(handler.HeadBucketHandler).Name("s3:HeadBucket")
		// DeleteMultipleObjects
		router.Methods(http.MethodPost).HandlerFunc(handler.DeleteMultipleObjectsHandler).Queries("delete", "").Name("s3:DeleteObjects")
		// DeleteBucketPolicy
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketPolicyHandler).Queries("policy", "").Name("s3:DeleteBucketPolicy")
		// DeleteBucketReplication
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketReplicationConfigHandler).Queries("replication", "").Name("s3:DeleteBucketReplication")
		// DeleteBucketLifecycle
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketLifecycleHandler).Queries("lifecycle", "").Name("s3:DeleteBucketLifecycleConfiguration")
		// DeleteBucketEncryption
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketEncryptionHandler).Queries("encryption", "").Name("s3:DeleteBucketEncryption")
		// DeleteBucket
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketHandler).Name("s3:DeleteBucket")

		// Register rejected bucket APIs
		for _, r := range handler.RejectedBucketAPIs {
			router.Methods(r.Methods...).
				HandlerFunc(handler.NotImplementedHandler).
				Queries(r.Queries...)
		}
		// S3 ListObjectsV1 (Legacy)
		router.Methods(http.MethodGet).HandlerFunc(handler.ListObjectsV1Handler).Name("s3:ListObjects")
	}

	// ListBuckets
	// 使用Path("/")配置根路径路由
	ar.Methods(http.MethodGet).Path("/").HandlerFunc(handler.ListBucketsHandler).Name("s3:ListBuckets")
	// 保留双斜杠路径的ListBuckets路由以兼容某些S3浏览器
	ar.Methods(http.MethodGet).Path("//").HandlerFunc(handler.ListBucketsHandler).Name("s3:ListBuckets")

}
