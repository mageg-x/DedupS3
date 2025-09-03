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
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/mageg-x/boulder/meta"

	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/service/object"

	"github.com/mageg-x/boulder/internal/logger"
)

func HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: HeadObjectHandler")
	//logger.GetLogger("boulder").Infof("head obect header %#v", r.Header)
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("boulder").Errorf("invalid object name: %s", objectKey)
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
		logger.GetLogger("boulder").Errorf("object service not initialized")
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
		logger.GetLogger("boulder").Errorf("object %s not found err: %v", objectKey, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	if objInfo == nil {
		logger.GetLogger("boulder").Errorf("object %s not found", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}

	logger.GetLogger("boulder").Debugf("headObject object %#v", objInfo)

	// If-Match
	if ifMatch != "" && string(objInfo.ETag) != ifMatch {
		logger.GetLogger("boulder").Errorf("Object %s is not matched with If-Match ETag %s:%s", objectKey, ifMatch, objInfo.ETag)
		xhttp.WriteAWSErr(w, r, xhttp.ErrPreconditionFailed)
		return
	}

	// If-None-Match
	if ifnoneMatch != "" && string(objInfo.ETag) != ifnoneMatch {
		logger.GetLogger("boulder").Errorf("object %s is not matched with If-None-Match ETag %s:%s", objectKey, ifnoneMatch, objInfo.ETag)
		xhttp.WriteAWSErr(w, r, xhttp.ERRNotModify)
		return
	}

	// If-Modified-Since
	if ifmodifiedSince != "" {
		if since, err := http.ParseTime(ifmodifiedSince); err == nil {
			if !objInfo.LastModified.After(since) {
				logger.GetLogger("boulder").Errorf("object %s is not last modified since %s", objectKey, since)
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
	logger.GetLogger("boulder").Infof("API called: GetObjectAttributesHandler")

	w.WriteHeader(http.StatusOK)
}

// GetObjectACLHandler 处理 GET Object ACL 请求 (Dummy)
func GetObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetObjectACLHandler")
	// TODO: 实现 GET Object ACL 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
}

// PutObjectACLHandler 处理 PUT Object ACL 请求 (Dummy)
func PutObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutObjectACLHandler")
	// TODO: 实现 PUT Object ACL 逻辑 (Dummy)
	w.WriteHeader(http.StatusOK)
}

// GetObjectTaggingHandler 处理 GET Object Tagging 请求
func GetObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetObjectTaggingHandler")
	// TODO: 实现 GET Object Tagging 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutObjectTaggingHandler 处理 PUT Object Tagging 请求
func PutObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutObjectTaggingHandler")
	// TODO: 实现 PUT Object Tagging 逻辑
	w.WriteHeader(http.StatusOK)
}

// DeleteObjectTaggingHandler 处理 DELETE Object Tagging 请求
func DeleteObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteObjectTaggingHandler")
	// TODO: 实现 DELETE Object Tagging 逻辑
	w.WriteHeader(http.StatusOK)
}

// SelectObjectContentHandler 处理 SELECT Object Content 请求
func SelectObjectContentHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: SelectObjectContentHandler")
	// TODO: 实现 SELECT Object Content 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetObjectRetentionHandler 处理 GET Object Retention 请求
func GetObjectRetentionHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetObjectRetentionHandler")
	// TODO: 实现 GET Object Retention 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetObjectLegalHoldHandler 处理 GET Object Legal Hold 请求
func GetObjectLegalHoldHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetObjectLegalHoldHandler")
	// TODO: 实现 GET Object Legal Hold 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetObjectLambdaHandler 处理 GET Object with Lambda ARN 请求
func GetObjectLambdaHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetObjectLambdaHandler")
	// TODO: 实现 GET Object with Lambda ARN 逻辑
	w.WriteHeader(http.StatusOK)
}

// GetObjectHandler 获取对象
func GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: GetObjectHandler")
	//logger.GetLogger("boulder").Infof("head obect header %#v", r.Header)
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("boulder").Errorf("invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}
	// 解析 Range 头
	rangeHeadStr := r.Header.Get(xhttp.Range)
	rangeHead, err := xhttp.ParseRequestRangeSpec(rangeHeadStr)
	if err != nil && rangeHeadStr != "" {
		logger.GetLogger("boulder").Errorf("invalid range header: %s, error: %v", rangeHeadStr, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidRange)
		return
	}

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("boulder").Errorf("object service not initialized")
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
		logger.GetLogger("boulder").Errorf("failed to fetch object %s: %v", objectKey, err)
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
			logger.GetLogger("boulder").Errorf("invalid range head: %s, error: %v", rangeHeadStr, err)
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
		logger.GetLogger("boulder").Errorf("write response body failed: %v", err)
		// 注意：此时可能已写 header，不能写 error
		return
	}
}

// CopyObjectHandler 复制对象
func CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: CopyObjectHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("boulder").Errorf("invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	ifMatch := r.Header.Get(xhttp.IfMatch)
	ifMatch = strings.Trim(ifMatch, "\"")
	ifnoneMatch := r.Header.Get(xhttp.IfNoneMatch)
	ifnoneMatch = strings.Trim(ifnoneMatch, "\"")
	ifmodifiedSince := r.Header.Get(xhttp.IfModifiedSince)
	CopySourceIfMatch := r.Header.Get(xhttp.AmzCopySourceIfMatch)
	CopySourceIfNoneMatch := r.Header.Get(xhttp.AmzCopySourceIfNoneMatch)
	CopySourceIfModifiedSince := r.Header.Get(xhttp.AmzCopySourceIfModifiedSince)
	CopySourceIfUnmodifiedSince := r.Header.Get(xhttp.AmzCopySourceIfUnmodifiedSince)

	// 获取源桶 和对象
	cpSrcPath := r.Header.Get(xhttp.AmzCopySource)
	if u, err := url.Parse(cpSrcPath); err == nil {
		cpSrcPath = u.Path
	}
	cpSrcPath = strings.TrimPrefix(cpSrcPath, "/")
	m := strings.Index(cpSrcPath, "/")
	var srcBucket, srcObject string
	if m > 2 {
		srcBucket, srcObject = cpSrcPath[:m], cpSrcPath[m+len("/"):]
	}
	srcObject = utils.TrimLeadingSlash(srcObject)
	if srcObject == "" || srcBucket == "" {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidCopySource)
		return
	}

	dstSc := r.Header.Get(xhttp.AmzStorageClass)

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("boulder").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	obj, err := _os.CopyObject(srcBucket, srcObject, &object.BaseObjectParams{
		BucketName:                  bucket,
		ObjKey:                      objectKey,
		AccessKeyID:                 accessKeyID,
		StorageClass:                dstSc,
		IfMatch:                     ifMatch,
		IfNoneMatch:                 ifnoneMatch,
		IfModifiedSince:             ifmodifiedSince,
		CopySourceIfMatch:           CopySourceIfMatch,
		CopySourceIfNoneMatch:       CopySourceIfNoneMatch,
		CopySourceIfModifiedSince:   CopySourceIfModifiedSince,
		CopySourceIfUnmodifiedSince: CopySourceIfUnmodifiedSince,
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
		logger.GetLogger("boulder").Errorf("failed to copy object %s: %v", objectKey, err)
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
	logger.GetLogger("boulder").Infof("API called: PutObjectRetentionHandler")
	// TODO: 实现 PUT Object Retention 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutObjectLegalHoldHandler 处理 PUT Object Legal Hold 请求
func PutObjectLegalHoldHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutObjectLegalHoldHandler")
	// TODO: 实现 PUT Object Legal Hold 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutObjectExtractHandler 处理 PUT Object with auto-extract 请求
func PutObjectExtractHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutObjectExtractHandler")
	// TODO: 实现 PUT Object with auto-extract 逻辑
	w.WriteHeader(http.StatusOK)
}

// PutObjectHandler 上传对象
func PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutObjectHandler")
	//logger.GetLogger("boulder").Infof("putobect header %#v", r.Header)
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("boulder").Errorf("Invalid object name: %s", objectKey)
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
			logger.GetLogger("boulder").Errorf("Invalid storage class: %s", sc)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidStorageClass)
			return
		}
	}

	size := r.ContentLength

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("boulder").Errorf("Object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	obj, err := _os.PutObject(r.Body, r.Header, &object.BaseObjectParams{
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
		logger.GetLogger("boulder").Errorf("Error putting object: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	w.Header().Set(xhttp.ETag, fmt.Sprintf("\"%s\"", obj.ETag))
	w.WriteHeader(http.StatusOK)
}

// DeleteObjectHandler 删除对象
func DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: DeleteObjectHandler")
	logger.GetLogger("boulder").Infof("API called: PutObjectHandler")
	//logger.GetLogger("boulder").Infof("putobect header %#v", r.Header)
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("boulder").Errorf("Invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("boulder").Errorf("Object service not initialized")
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
	logger.GetLogger("boulder").Infof("API called: DeleteMultipleObjectsHandler")
	// TODO: 实现 Delete Multiple Objects 逻辑
	w.WriteHeader(http.StatusOK)
}

// PostRestoreObjectHandler 处理 POST Restore Object 请求
func PostRestoreObjectHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PostRestoreObjectHandler")
	// TODO: 实现 POST Restore Object 逻辑
	w.WriteHeader(http.StatusOK)
}

func ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Infof("API called: ListObjectsV1Handler")

	bucket, _, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidBucketName(bucket); err != nil {
		logger.GetLogger("boulder").Errorf("invalid bucket name: %s", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketName)
		return
	}
	query := r.URL.Query()
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
	logger.GetLogger("boulder").Infof("get query %#v", query)
	if prefix != "" {
		if err := utils.CheckValidObjectNamePrefix(prefix); err != nil {
			logger.GetLogger("boulder").Errorf("invalid prefix: %s", prefix)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
			return
		}
	}

	if encodingType != "" {
		if !strings.EqualFold(encodingType, "url") {
			logger.GetLogger("boulder").Errorf("invalid encoding-type: %s", encodingType)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidEncodingMethod)
			return
		}
		encodingType = "url" // ✅ 标准化
	}

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("boulder").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	objects, err := _os.ListObjects(bucket, accessKeyID, prefix, marker, delimiter, maxkeys)
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}
	if err != nil {
		logger.GetLogger("boulder").Errorf("Error listing objects: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	resp := object.ListObjectsResponse{
		XMLName:  xml.Name{Local: "ListBucketResult"},
		XMLNS:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:     bucket,
		Contents: make([]object.ObjectContent, 0, len(objects)),
	}

	isTruncated := len(objects) > maxkeys

	resp.IsTruncated = &isTruncated
	if isTruncated && len(objects) > 0 {
		lastKey := objects[len(objects)-1].Key
		resp.NextMarker = &lastKey
	}
	if isTruncated {
		objects = objects[:maxkeys]
	}

	// 只设置非空的可选字段
	if prefix != "" {
		resp.Prefix = &prefix
	}
	if marker != "" {
		resp.Marker = &marker
	}
	if delimiter != "" {
		resp.Delimiter = &delimiter
	}
	if encodingType != "" {
		resp.Delimiter = &encodingType
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

	// 如果 encoding-type=url，对所有字符串进行 URL 编码
	if encodingType == "url" {
		encode := func(s string) string {
			return url.QueryEscape(s)
		}

		if resp.Prefix != nil {
			encoded := encode(*resp.Prefix)
			resp.Prefix = &encoded
		}
		if resp.Marker != nil {
			encoded := encode(*resp.Marker)
			resp.Marker = &encoded
		}
		if resp.NextMarker != nil {
			encoded := encode(*resp.NextMarker)
			resp.NextMarker = &encoded
		}

		// 编码 Contents.Key
		for i := range resp.Contents {
			encoded := encode(resp.Contents[i].Key)
			resp.Contents[i].Key = encoded
		}
	}

	xhttp.WriteAWSSuc(w, r, resp)
}

// ListObjectsV2MHandler 处理 List Objects V2 with metadata 请求
func ListObjectsV2MHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: ListObjectsV2MHandler")
	// TODO: 实现 List Objects V2 逻辑
	w.WriteHeader(http.StatusOK)
}

// ListObjectsV2Handler 处理 List Objects V2 请求
func ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Infof("API called: ListObjectsV2Handler")

	bucket, _, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidBucketName(bucket); err != nil {
		logger.GetLogger("boulder").Errorf("invalid bucket name: %s", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketName)
		return
	}
	query := r.URL.Query()
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
	logger.GetLogger("boulder").Infof("get query %#v", query)
	if prefix != "" {
		if err := utils.CheckValidObjectNamePrefix(prefix); err != nil {
			logger.GetLogger("boulder").Errorf("invalid prefix: %s", prefix)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
			return
		}
	}

	if encodingType != "" {
		if !strings.EqualFold(encodingType, "url") {
			logger.GetLogger("boulder").Errorf("invalid encoding-type: %s", encodingType)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidEncodingMethod)
			return
		}
		encodingType = "url" // ✅ 标准化
	}

	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("boulder").Errorf("object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	objects, err := _os.ListObjectsV2(bucket, accessKeyID, prefix, continuationToken, startAfter, delimiter, maxkeys)
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}
	if err != nil {
		logger.GetLogger("boulder").Errorf("Error listing objects: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	resp := &object.ListObjectsV2Response{
		XMLName:  xml.Name{Local: "ListBucketResult"},
		XMLNS:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:     bucket,
		KeyCount: 0,
		Contents: make([]object.ObjectContent, 0),
	}

	isTruncated := len(objects) > maxkeys

	resp.IsTruncated = &isTruncated
	if isTruncated && len(objects) > 0 {
		lastKey := objects[len(objects)-1].Key
		token := base64.StdEncoding.EncodeToString([]byte(lastKey))
		resp.NextContinuationToken = &token
	}
	if isTruncated {
		objects = objects[:maxkeys]
	}
	resp.KeyCount = len(objects)
	// 只设置非空的可选字段
	if prefix != "" {
		resp.Prefix = &prefix
	}
	if startAfter != "" {
		resp.StartAfter = &startAfter
	}
	if delimiter != "" {
		resp.Delimiter = &delimiter
	}
	if encodingType != "" {
		resp.Delimiter = &encodingType
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

	// 如果 encoding-type=url，对所有字符串进行 URL 编码
	if encodingType == "url" {
		encode := func(s string) string {
			return url.QueryEscape(s)
		}

		if resp.Prefix != nil {
			encoded := encode(*resp.Prefix)
			resp.Prefix = &encoded
		}
		if resp.StartAfter != nil {
			encoded := encode(*resp.StartAfter)
			resp.StartAfter = &encoded
		}

		// 编码 Contents.Key
		for i := range resp.Contents {
			encoded := encode(resp.Contents[i].Key)
			resp.Contents[i].Key = encoded
		}
	}
	xhttp.WriteAWSSuc(w, r, resp)
}

// ListObjectVersionsMHandler 处理 List Object Versions with metadata 请求
func ListObjectVersionsMHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: ListObjectVersionsMHandler")
	// TODO: 实现 List Object Versions with metadata 逻辑
	w.WriteHeader(http.StatusOK)
}

// ListObjectVersionsHandler 处理 List Object Versions 请求
func ListObjectVersionsHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: ListObjectVersionsHandler")
	// TODO: 实现 List Object Versions 逻辑
	w.WriteHeader(http.StatusOK)
}
