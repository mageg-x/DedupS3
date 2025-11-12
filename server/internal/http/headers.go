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
package http

// Standard S3 HTTP response constants
const (
	LastModified       = "Last-Modified"
	Date               = "Date"
	ETag               = "ETag"
	ContentType        = "Content-Type"
	ContentMD5         = "Content-Md5"
	ContentEncoding    = "Content-Encoding"
	Expires            = "Expires"
	ContentLength      = "Content-Length"
	ContentLanguage    = "Content-Language"
	ContentRange       = "Content-Range"
	Connection         = "Connection"
	AcceptRanges       = "Accept-Ranges"
	AmzBucketRegion    = "X-Amz-Bucket-Region"
	ServerInfo         = "Server"
	RetryAfter         = "Retry-After"
	Location           = "Location"
	CacheControl       = "Cache-Control"
	ContentDisposition = "Content-Disposition"
	Authorization      = "Authorization"
	Action             = "Action"
	Range              = "Range"
)

// Non standard S3 HTTP response constants
const (
	XCache       = "X-Cache"
	XCacheLookup = "X-Cache-Lookup"
)

// Standard S3 HTTP request constants
const (
	IfModifiedSince   = "If-Modified-Since"
	IfUnmodifiedSince = "If-Unmodified-Since"
	IfMatch           = "If-Match"
	IfNoneMatch       = "If-None-Match"

	// Request tags used in GetObjectAttributes
	Checksum     = "Checksum"
	StorageClass = "StorageClass"
	ObjectSize   = "ObjectSize"
	ObjectParts  = "ObjectParts"

	// S3 storage class
	AmzStorageClass = "x-amz-storage-class"

	// S3 object version ID
	AmzVersionID    = "x-amz-version-id"
	AmzDeleteMarker = "x-amz-delete-marker"

	// S3 object tagging
	AmzObjectTagging = "X-Amz-Tagging"
	AmzTagCount      = "x-amz-tagging-count"
	AmzTagDirective  = "X-Amz-Tagging-Directive"

	// S3 transition restore
	AmzRestore            = "x-amz-restore"
	AmzRestoreExpiryDays  = "X-Amz-Restore-Expiry-Days"
	AmzRestoreRequestDate = "X-Amz-Restore-Request-Date"
	AmzRestoreOutputPath  = "x-amz-restore-output-path"

	// S3 extensions
	AmzCopySourceIfModifiedSince   = "x-amz-copy-source-if-modified-since"
	AmzCopySourceIfUnmodifiedSince = "x-amz-copy-source-if-unmodified-since"

	AmzCopySourceIfNoneMatch = "x-amz-copy-source-if-none-match"
	AmzCopySourceIfMatch     = "x-amz-copy-source-if-match"

	AmzCopySource                 = "X-Amz-Copy-Source"
	AmzCopySourceVersionID        = "X-Amz-Copy-Source-Version-Id"
	AmzCopySourceRange            = "X-Amz-Copy-Source-Range"
	AmzMetadataDirective          = "X-Amz-Metadata-Directive"
	AmzObjectLockMode             = "X-Amz-Object-Lock-Mode"
	AmzObjectLockRetainUntilDate  = "X-Amz-Object-Lock-Retain-Until-Date"
	AmzObjectLockLegalHold        = "X-Amz-Object-Lock-Legal-Hold"
	AmzObjectLockBypassGovernance = "X-Amz-Bypass-Governance-Retention"
	AmzBucketReplicationStatus    = "X-Amz-Replication-Status"

	// AmzSnowballExtract will trigger unpacking of an archive content
	AmzSnowballExtract = "X-Amz-Meta-Snowball-Auto-Extract"

	// Object lock enabled
	AmzObjectLockEnabled = "x-amz-bucket-object-lock-enabled"

	// Multipart parts count
	AmzMpPartsCount = "x-amz-mp-parts-count"

	// Object date/time of expiration
	AmzExpiration = "x-amz-expiration"

	// Dummy putBucketACL
	AmzACL = "x-amz-acl"

	// Signature V4 related constants.
	AmzContentSha256        = "X-Amz-Content-Sha256"
	AmzDate                 = "X-Amz-Date"
	AmzAlgorithm            = "X-Amz-Algorithm"
	AmzExpires              = "X-Amz-Expires"
	AmzSignedHeaders        = "X-Amz-SignedHeaders"
	AmzSignature            = "X-Amz-Signature"
	AmzCredential           = "X-Amz-Credential"
	AmzSecurityToken        = "X-Amz-Security-Token"
	AmzDecodedContentLength = "X-Amz-Decoded-Content-Length"
	AmzTrailer              = "X-Amz-Trailer"
	AmzMaxParts             = "X-Amz-Max-Parts"
	AmzPartNumberMarker     = "X-Amz-Part-Number-Marker"

	// Constants used for GetObjectAttributes and GetObjectVersionAttributes
	AmzObjectAttributes = "X-Amz-Object-Attributes"

	AmzMetaUnencryptedContentLength = "X-Amz-Meta-X-Amz-Unencrypted-Content-Length"
	AmzMetaUnencryptedContentMD5    = "X-Amz-Meta-X-Amz-Unencrypted-Content-Md5"

	// AWS server-side encryption headers for SSE-S3, SSE-KMS and SSE-C.
	AmzServerSideEncryption                      = "X-Amz-Server-Side-Encryption"
	AmzServerSideEncryptionKmsID                 = AmzServerSideEncryption + "-Aws-Kms-Key-Id"
	AmzServerSideEncryptionKmsContext            = AmzServerSideEncryption + "-Context"
	AmzServerSideEncryptionCustomerAlgorithm     = AmzServerSideEncryption + "-Customer-Algorithm"
	AmzServerSideEncryptionCustomerKey           = AmzServerSideEncryption + "-Customer-Key"
	AmzServerSideEncryptionCustomerKeyMD5        = AmzServerSideEncryption + "-Customer-Key-Md5"
	AmzServerSideEncryptionCopyCustomerAlgorithm = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm"
	AmzServerSideEncryptionCopyCustomerKey       = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key"
	AmzServerSideEncryptionCopyCustomerKeyMD5    = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5"

	AmzEncryptionAES = "AES256"
	AmzEncryptionKMS = "aws:kms"

	// Signature v2 related constants
	AmzSignatureV2 = "Signature"
	AmzAccessKeyID = "AWSAccessKeyId"

	// Response request id.
	AmzRequestID      = "x-amz-request-id"
	AmzRequestHostID  = "x-amz-id-2"
	AmzRequestCharged = "x-amz-request-charged"
	AmzRequestPayer   = "x-amz-request-payer"

	// Content Checksums
	AmzChecksumAlgo           = "x-amz-checksum-algorithm"
	AmzChecksumCRC32          = "x-amz-checksum-crc32"
	AmzChecksumCRC32C         = "x-amz-checksum-crc32c"
	AmzChecksumSHA1           = "x-amz-checksum-sha1"
	AmzChecksumSHA256         = "x-amz-checksum-sha256"
	AmzChecksumCRC64NVME      = "x-amz-checksum-crc64nvme"
	AmzChecksumMode           = "x-amz-checksum-mode"
	AmzChecksumType           = "x-amz-checksum-type"
	AmzChecksumTypeFullObject = "FULL_OBJECT"
	AmzChecksumTypeComposite  = "COMPOSITE"

	// S3 Express API related constant reject it.
	AmzWriteOffsetBytes = "x-amz-write-offset-bytes"

	// Post Policy related
	AmzMetaUUID    = "X-Amz-Meta-Uuid"
	AmzMetaName    = "X-Amz-Meta-Name"
	AMZMetPrefix   = "X-Amz-Meta-"
	AMZMaxMetaSize = 2 * 1024 // 2KB
	// SUBNET related
	SubnetAPIKey = "x-subnet-api-key"

	// Rename object related headers
	AmzRenameSource                  = "x-amz-rename-source"
	AmzRenameSourceIfMatch           = "x-amz-rename-source-if-match"
	AmzRenameSourceIfNoneMatch       = "x-amz-rename-source-if-none-match"
	AmzRenameSourceIfModifiedSince   = "x-amz-rename-source-if-modified-since"
	AmzRenameSourceIfUnmodifiedSince = "x-amz-rename-source-if-unmodified-since"
	AmzClientToken                   = "x-amz-client-token"
	AmzBucketObjectLockToken         = "x-amz-bucket-object-lock-token"
)

// Common http query params S3 API
const (
	VersionID = "versionId"

	PartNumber = "partNumber"

	UploadID = "uploadId"
)
