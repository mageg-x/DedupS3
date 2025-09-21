package router

import (
	"github.com/gorilla/mux"
	"net/http"

	"github.com/mageg-x/boulder/handler"
	"github.com/mageg-x/boulder/internal/config"
	xhttp "github.com/mageg-x/boulder/internal/http"
)

func registerAPIRouter(mr *mux.Router) {
	// init api router
	ar := mr.PathPrefix("/").Subrouter()
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
		router.Methods(http.MethodHead).Path("/{object:.+}").HandlerFunc(handler.HeadObjectHandler).Name("HeadObject")

		// GetObjectAttributes
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectAttributesHandler).Queries("attributes", "").Name("GetObjectAttributes")

		// CopyObjectPart
		router.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(handler.CopyObjectPartHandler).Queries("partNumber", "{partNumber:.*}", "uploadId", "{uploadId:.*}").Name("UploadPartCopy")
		// PutObjectPart
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectPartHandler).Queries("partNumber", "{partNumber:.*}", "uploadId", "{uploadId:.*}").Name("UploadPart")
		// ListObjectParts
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.ListObjectPartsHandler).Queries("uploadId", "{uploadId:.*}").Name("ListParts")
		// CompleteMultipartUpload
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(handler.CompleteMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Name("CompleteMultipartUpload")
		// NewMultipartUpload
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(handler.NewMultipartUploadHandler).Queries("uploads", "").Name("CreateMultipartUpload")
		// AbortMultipartUpload
		router.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(handler.AbortMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Name("AbortMultipartUpload")
		// GetObjectACL - this is a dummy call.
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectACLHandler).Queries("acl", "").Name("GetObjectAcl")
		// PutObjectACL - this is a dummy call.
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectACLHandler).Queries("acl", "").Name("PutObjectAcl")
		// GetObjectTagging
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectTaggingHandler).Queries("tagging", "").Name("GetObjectTagging")
		// PutObjectTagging
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectTaggingHandler).Queries("tagging", "").Name("PutObjectTagging")
		// DeleteObjectTagging
		router.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(handler.DeleteObjectTaggingHandler).Queries("tagging", "").Name("DeleteObjectTagging")
		// SelectObjectContent
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(handler.SelectObjectContentHandler).Queries("select", "").Queries("select-type", "2").Name("SelectObjectContent")
		// GetObjectRetention
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectRetentionHandler).Queries("retention", "").Name("GetObjectRetention")
		// GetObjectLegalHold
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectLegalHoldHandler).Queries("legal-hold", "").Name("GetObjectLegalHold")
		// GetObject with lambda ARNs
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectLambdaHandler).Queries("lambdaArn", "{lambdaArn:.*}").Name("GetObjectWithLambda")
		// GetObject
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(handler.GetObjectHandler).Name("GetObject")
		// CopyObject
		router.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(handler.CopyObjectHandler).Name("CopyObject")
		// PutObjectRetention
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectRetentionHandler).Queries("retention", "").Name("PutObjectRetention")
		// PutObjectLegalHold
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectLegalHoldHandler).Queries("legal-hold", "").Name("PutObjectLegalHold")

		// PutObject with auto-extract support for zip
		router.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzSnowballExtract, "true").HandlerFunc(handler.PutObjectExtractHandler).Name("PutObjectExtract")

		// AppendObject to be rejected
		router.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzWriteOffsetBytes, "").HandlerFunc(handler.ErrorResponseHandler).Name("AppendObject") // Assuming errorResponseHandler is the final handler

		// RenameObject
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.RenameObjectHandler).Queries("renameObject", "").Name("RenameObject")

		// PutObject
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(handler.PutObjectHandler).Name("PutObject")

		// DeleteObject
		router.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(handler.DeleteObjectHandler).Name("DeleteObject")

		// PostRestoreObject
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(handler.RestoreObjectHandler).Queries("restore", "").Name("RestoreObject")

		// Bucket operations

		// GetBucketLocation
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketLocationHandler).Queries("location", "").Name("GetBucketLocation")
		// GetBucketPolicy
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketPolicyHandler).Queries("policy", "").Name("GetBucketPolicy")
		// GetBucketLifecycle
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketLifecycleHandler).Queries("lifecycle", "").Name("GetBucketLifecycleConfiguration")
		// GetBucketEncryption
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketEncryptionHandler).Queries("encryption", "").Name("GetBucketEncryption")
		// GetBucketObjectLockConfig
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketObjectLockConfigHandler).Queries("object-lock", "").Name("GetObjectLockConfiguration")
		// GetBucketReplicationConfig
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketReplicationConfigHandler).Queries("replication", "").Name("GetBucketReplication")
		// GetBucketVersioning
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketVersioningHandler).Queries("versioning", "").Name("GetBucketVersioning")
		// GetBucketNotification
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketNotificationHandler).Queries("notification", "").Name("GetBucketNotificationConfiguration")

		// Dummy Bucket Calls
		// GetBucketACL -- this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketACLHandler).Queries("acl", "").Name("GetBucketAcl")
		// PutBucketACL -- this is a dummy call.
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketACLHandler).Queries("acl", "").Name("PutBucketAcl")
		// GetBucketCors - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketCorsHandler).Queries("cors", "").Name("GetBucketCors")
		// PutBucketCors - this is a dummy call.
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketCorsHandler).Queries("cors", "").Name("PutBucketCors")
		// DeleteBucketCors - this is a dummy call.
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketCorsHandler).Queries("cors", "").Name("DeleteBucketCors")
		// GetBucketWebsiteHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketWebsiteHandler).Queries("website", "").Name("GetBucketWebsite")
		// GetBucketAccelerateHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketAccelerateHandler).Queries("accelerate", "").Name("GetBucketAccelerateConfiguration")
		// GetBucketRequestPaymentHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketRequestPaymentHandler).Queries("requestPayment", "").Name("GetBucketRequestPayment")
		// GetBucketLoggingHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketLoggingHandler).Queries("logging", "").Name("GetBucketLogging")

		// GetBucketTaggingHandler
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketTaggingHandler).Queries("tagging", "").Name("GetBucketTagging")
		// DeleteBucketWebsiteHandler
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketWebsiteHandler).Queries("website", "").Name("DeleteBucketWebsite")
		// DeleteBucketTaggingHandler
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketTaggingHandler).Queries("tagging", "").Name("DeleteBucketTagging")

		// ListMultipartUploads
		router.Methods(http.MethodGet).HandlerFunc(handler.ListMultipartUploadsHandler).Queries("uploads", "").Name("ListMultipartUploads")
		// ListObjectsV2M
		router.Methods(http.MethodGet).HandlerFunc(handler.ListObjectsV2MHandler).Queries("list-type", "2", "metadata", "true").Name("ListObjectsV2WithMetadata")
		// ListObjectsV2
		router.Methods(http.MethodGet).HandlerFunc(handler.ListObjectsV2Handler).Queries("list-type", "2").Name("ListObjectsV2")
		// ListObjectVersions
		router.Methods(http.MethodGet).HandlerFunc(handler.ListObjectVersionsMHandler).Queries("versions", "", "metadata", "true").Name("ListObjectVersionsWithMetadata")
		// ListObjectVersions
		router.Methods(http.MethodGet).HandlerFunc(handler.ListObjectVersionsHandler).Queries("versions", "").Name("ListObjectVersions")
		// GetBucketPolicyStatus
		router.Methods(http.MethodGet).HandlerFunc(handler.GetBucketPolicyStatusHandler).Queries("policyStatus", "").Name("GetBucketPolicyStatus")
		// PutBucketLifecycle
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketLifecycleHandler).Queries("lifecycle", "").Name("PutBucketLifecycleConfiguration")
		// PutBucketReplicationConfig
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketReplicationConfigHandler).Queries("replication", "").Name("PutBucketReplication")
		// PutBucketEncryption
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketEncryptionHandler).Queries("encryption", "").Name("PutBucketEncryption")

		// PutBucketPolicy
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketPolicyHandler).Queries("policy", "").Name("PutBucketPolicy")

		// PutBucketObjectLockConfig
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketObjectLockConfigHandler).Queries("object-lock", "").Name("PutObjectLockConfiguration")
		// PutBucketTaggingHandler
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketTaggingHandler).Queries("tagging", "").Name("PutBucketTagging")
		// PutBucketVersioning
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketVersioningHandler).Queries("versioning", "").Name("PutBucketVersioning")
		// PutBucketNotification
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketNotificationHandler).Queries("notification", "").Name("PutBucketNotificationConfiguration")

		// PutBucket
		router.Methods(http.MethodPut).HandlerFunc(handler.PutBucketHandler).Name("CreateBucket")
		// HeadBucket
		router.Methods(http.MethodHead).HandlerFunc(handler.HeadBucketHandler).Name("HeadBucket")
		// DeleteMultipleObjects
		router.Methods(http.MethodPost).HandlerFunc(handler.DeleteMultipleObjectsHandler).Queries("delete", "").Name("DeleteObjects")
		// DeleteBucketPolicy
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketPolicyHandler).Queries("policy", "").Name("DeleteBucketPolicy")
		// DeleteBucketReplication
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketReplicationConfigHandler).Queries("replication", "").Name("DeleteBucketReplication")
		// DeleteBucketLifecycle
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketLifecycleHandler).Queries("lifecycle", "").Name("DeleteBucketLifecycleConfiguration")
		// DeleteBucketEncryption
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketEncryptionHandler).Queries("encryption", "").Name("DeleteBucketEncryption")
		// DeleteBucket
		router.Methods(http.MethodDelete).HandlerFunc(handler.DeleteBucketHandler).Name("DeleteBucket")

		// Register rejected bucket APIs
		for _, r := range handler.RejectedBucketAPIs {
			router.Methods(r.Methods...).
				HandlerFunc(handler.NotImplementedHandler).
				Queries(r.Queries...)
		}
		// S3 ListObjectsV1 (Legacy)
		router.Methods(http.MethodGet).HandlerFunc(handler.ListObjectsV1Handler).Name("ListObjects")
	}

	// ListBuckets
	// 使用Path("/")配置根路径路由
	ar.Methods(http.MethodGet).Path("/").HandlerFunc(handler.ListBucketsHandler).Name("ListBuckets")
	// 保留双斜杠路径的ListBuckets路由以兼容某些S3浏览器
	ar.Methods(http.MethodGet).Path("//").HandlerFunc(handler.ListBucketsHandler).Name("ListBuckets")

}
