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
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mageg-x/boulder/internal/aws"

	"github.com/mageg-x/boulder/meta"

	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/service/multipart"
	"github.com/mageg-x/boulder/service/object"

	"github.com/mageg-x/boulder/internal/logger"
)

// CompleteMultipartUploadHandler 完成分段上传
func CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: CompleteMultipartUploadHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	uploadID := r.URL.Query().Get("uploadId")
	ifMatch := r.Header.Get(xhttp.IfMatch)
	ifMatch = strings.Trim(ifMatch, "\"")
	ifnoneMatch := r.Header.Get(xhttp.IfNoneMatch)
	ifnoneMatch = strings.Trim(ifnoneMatch, "\"")

	// 解析请求体中的XML内容
	defer r.Body.Close() // 先 defer，再读

	var completeXML meta.CompleteMultipartUpload
	err := xml.NewDecoder(r.Body).Decode(&completeXML)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to parse request body: %v", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedXML)
		return
	}

	// 转换为PartETag数组
	parts := make([]meta.PartETag, 0, len(completeXML.Parts))
	for _, part := range completeXML.Parts {
		parts = append(parts, meta.PartETag{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}

	_mps := multipart.GetMultiPartService()
	if _mps == nil {
		logger.GetLogger("boulder").Errorf("multipart service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	obj, err := _mps.CompleteMultipartUpload(parts, &object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
		UploadID:    uploadID,
		IfMatch:     ifMatch,
		IfNoneMatch: ifnoneMatch,
	})

	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchUpload)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchUpload)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidPart)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidPart)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidPartOrder)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidPartOrder)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidQueryParams)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidQueryParams)
		return
	}
	if err != nil {
		logger.GetLogger("boulder").Errorf("error completing multipart upload: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 构建响应
	w.Header().Set(xhttp.ETag, fmt.Sprintf(`"%s"`, obj.ETag))
	w.Header().Set(xhttp.LastModified, obj.LastModified.Format(http.TimeFormat))
	resp := multipart.CompleteMultipartUploadResult{
		XMLNS:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Location: fmt.Sprintf("http://%s/%s", r.Host, utils.TrimLeadingSlash(objectKey)),
		Bucket:   bucket,
		Key:      objectKey,
		ETag:     obj.ETag,
	}
	xhttp.WriteAWSSuc(w, r, resp)
}

// NewMultipartUploadHandler 创建分段上传
func NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: NewMultipartUploadHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	if err := utils.CheckValidBucketName(bucket); err != nil {
		logger.GetLogger("boulder").Errorf("Invalid bucket name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketName)
		return
	}

	if err := utils.CheckValidObjectName(objectKey); err != nil {
		logger.GetLogger("boulder").Errorf("Invalid object name: %s", objectKey)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidObjectName)
		return
	}

	_mps := multipart.GetMultiPartService()
	if _mps == nil {
		logger.GetLogger("boulder").Errorf("Object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	upload, err := _mps.CreateMultipartUpload(r.Header, &object.BaseObjectParams{
		AccessKeyID: accessKeyID,
		BucketName:  bucket,
		ObjKey:      objectKey,
	})
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidStorageClass)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidStorageClass)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrPreconditionFailed)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrPreconditionFailed)
		return
	}
	if err != nil {
		logger.GetLogger("boulder").Errorf("Error creating multipart upload: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	resp := &multipart.InitiateMultipartUploadResult{
		XMLNS:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   upload.Bucket,
		Key:      upload.Key,
		UploadId: upload.UploadID,
	}

	xhttp.WriteAWSSuc(w, r, resp)
}

// AbortMultipartUploadHandler AbortMultipartUpload  中止分段上传
func AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Debugf("API called: AbortMultipartUploadHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	uploadID := r.URL.Query().Get("uploadId")

	_mps := multipart.GetMultiPartService()
	if _mps == nil {
		logger.GetLogger("boulder").Errorf("Object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	err := _mps.AbortMultipartUpload(&object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
		UploadID:    uploadID,
	})
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchUpload)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchUpload)
		return
	}
	if err != nil {
		logger.GetLogger("boulder").Errorf("Error aborting multipart upload: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListMultipartUploadsHandler  ListMultipartUploads 列出bucket下所有的正在上传的uploadid
func ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Infof("API called: ListMultipartUploadsHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	// 验证bucket名称
	if err := utils.CheckValidBucketName(bucket); err != nil || accessKeyID == "" {
		logger.GetLogger("boulder").Errorf("Invalid bucket name: %s", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketName)
		return
	}
	// 解析查询参数
	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	keyMarker := query.Get("key-marker")
	uploadIDMarker := query.Get("upload-id-marker")
	maxUploadsStr := query.Get("max-uploads")
	encodingType := query.Get("encoding-type")

	// 解析MaxUploads参数
	maxUploads := int64(1000) // S3默认值
	if maxUploadsStr != "" {
		parsedMaxUploads, err := strconv.ParseInt(maxUploadsStr, 10, 64)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Invalid max-uploads: %s", maxUploadsStr)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}
		// S3限制max-uploads最大为1000
		if parsedMaxUploads > 0 && parsedMaxUploads <= 1000 {
			maxUploads = parsedMaxUploads
		}
	}
	// 设置默认的MaxUploads值
	if maxUploads < 0 || maxUploads > 1000 {
		maxUploads = 1000
	}
	// 验证encoding-type参数
	if encodingType != "" && encodingType != "url" {
		logger.GetLogger("boulder").Errorf("Invalid encoding-type: %s", encodingType)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
		return
	}

	// 获取MultiPartService实例
	_mps := multipart.GetMultiPartService()
	if _mps == nil {
		logger.GetLogger("boulder").Errorf("multipart service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 调用ListMultipartUploads方法
	uploads, err := _mps.ListMultipartUploads(&object.BaseObjectParams{
		BucketName:     bucket,
		ObjKey:         objectKey,
		AccessKeyID:    accessKeyID,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		Delimiter:      delimiter,
		Prefix:         prefix,
		Encodingtype:   encodingType,
		KeyMarker:      keyMarker,
	})

	// 处理错误
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidQueryParams)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidQueryParams)
		return
	}
	if err != nil {
		logger.GetLogger("boulder").Errorf("Error listing multipart uploads: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 构造响应结构
	result := multipart.ListMultipartUploadsResult{
		XMLName:        xml.Name{Local: "ListMultipartUploadsResult"},
		XMLNS:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:         bucket,
		MaxUploads:     int(maxUploads),
		Prefix:         prefix,
		Delimiter:      delimiter,
		EncodingType:   encodingType,
		KeyMarker:      keyMarker,
		UploadIdMarker: uploadIDMarker,
	}

	var xmlUploads []*multipart.Upload
	for _, u := range uploads {
		xmlUploads = append(xmlUploads, &multipart.Upload{
			Key:          u.Key,
			UploadId:     u.UploadID,
			StorageClass: u.StorageClass,
			Initiated:    u.Created.Format(time.RFC3339),
			Initiator:    u.Initiator,
			Owner:        u.Owner,
		})
	}
	result.Upload = xmlUploads

	if int64(len(uploads)) >= maxUploads && maxUploads > 0 && len(uploads) > 0 {
		result.IsTruncated = true
		last := uploads[len(uploads)-1]
		result.NextKeyMarker = last.Key
		result.NextUploadIdMarker = last.UploadID
	} else {
		result.IsTruncated = false
		result.NextKeyMarker = ""
		result.NextUploadIdMarker = ""
	}

	if encodingType == "url" {
		encoder := func(s string) string {
			return url.QueryEscape(s)
		}
		if result.Prefix != "" {
			result.Prefix = encoder(result.Prefix)
		}
		if result.KeyMarker != "" {
			result.KeyMarker = encoder(result.KeyMarker)
		}
		if result.NextKeyMarker != "" {
			result.NextKeyMarker = encoder(result.NextKeyMarker)
		}
		// 数据字段
		for _, u := range result.Upload {
			u.Key = encoder(u.Key) // Key 需要编码
			// UploadId 通常不含特殊字符，但 S3 也会编码
			u.UploadId = encoder(u.UploadId)
		}
	}

	xhttp.WriteAWSSuc(w, r, result)
}

// CopyObjectPartHandler  UploadPartCopy
func CopyObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Infof("API called: CopyObjectPartHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)

	// 验证bucket名称
	if err := utils.CheckValidBucketName(bucket); err != nil || accessKeyID == "" {
		logger.GetLogger("boulder").Errorf("Invalid bucket name: %s", bucket)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidBucketName)
		return
	}
	// 解析查询参数
	uploadID := r.URL.Query().Get("uploadId")
	partNumberStr := r.URL.Query().Get("partNumber")
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		logger.GetLogger("boulder").Errorf("Invalid part number: %s", partNumberStr)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidPartNumber)
		return
	}
	// 获取源对象信息
	source := r.Header.Get(xhttp.AmzCopySource)
	if source == "" {
		logger.GetLogger("boulder").Errorf("Missing x-amz-copy-source header")
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
		return
	}
	// 解析源对象路径，格式通常为 "/bucket/key"
	sourceParts := strings.SplitN(source, "/", 3)
	if len(sourceParts) < 3 {
		logger.GetLogger("boulder").Errorf("Invalid x-amz-copy-source format: %s", source)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
		return
	}
	sourceBucket := sourceParts[1]
	sourceKey := sourceParts[2]

	// 获取可选的范围参数
	rangeHeadStr := r.Header.Get(xhttp.AmzCopySourceRange)
	// 不支持 range 复制
	if rangeHeadStr != "" {
		logger.GetLogger("boulder").Errorf("invalid range header: %s, error: %v", rangeHeadStr, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidRange)
		return
	}

	SourceIfMatch := r.Header.Get(xhttp.AmzCopySourceIfMatch)
	SourceIfNoneMatch := r.Header.Get(xhttp.AmzCopySourceIfNoneMatch)
	SourceIfModifiedSince := r.Header.Get(xhttp.AmzCopySourceIfModifiedSince)
	SourceIfUnmodifiedSince := r.Header.Get(xhttp.AmzCopySourceIfUnmodifiedSince)

	// 获取MultiPartService实例
	_mps := multipart.GetMultiPartService()
	if _mps == nil {
		logger.GetLogger("boulder").Errorf("multipart service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	part, err := _mps.UploadPartCopy(sourceBucket, sourceKey, &object.BaseObjectParams{
		BucketName:              bucket,
		ObjKey:                  objectKey,
		AccessKeyID:             accessKeyID,
		UploadIDMarker:          uploadID,
		PartNumber:              int64(partNumber),
		SourceIfMatch:           SourceIfMatch,
		SourceIfNoneMatch:       SourceIfNoneMatch,
		SourceIfUnmodifiedSince: SourceIfUnmodifiedSince,
		SourceIfModifiedSince:   SourceIfModifiedSince,
	})

	if err != nil {
		logger.GetLogger("boulder").Errorf("%s/%s/%s/%d copy object part  failed : %v", bucket, objectKey, uploadID, partNumber, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	resp := multipart.CopyPartResult{
		XMLName:      xml.Name{Local: "CopyPartResult"},
		XMLNS:        "http://s3.amazonaws.com/doc/2006-03-01/",
		ETag:         part.ETag,
		LastModified: part.LastModified,
	}
	xhttp.WriteAWSSuc(w, r, &resp)
}

// PutObjectPartHandler UploadPart 请求
func PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: PutObjectPartHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	uploadID := r.URL.Query().Get("uploadId")
	partNumberStr := r.URL.Query().Get("partNumber")
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		logger.GetLogger("boulder").Errorf("Invalid part number: %s", partNumberStr)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidPartNumber)
		return
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
			logger.GetLogger("boulder").Errorf("Invalid X-Amz-Decoded-Content-Length: %s", contentLenStr)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidDigest) // 或 ErrMalformedRequestBody
			return
		}
	}
	if contentLength < 0 {
		logger.GetLogger("boulder").Errorf("Negative content length: %d", contentLength)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidDigest)
		return
	}

	// content Md5
	contentMd5 := r.Header.Get(xhttp.ContentMD5)
	// 去掉前后的双引号
	contentMd5 = strings.Trim(contentMd5, "\"")

	_mps := multipart.GetMultiPartService()
	if _mps == nil {
		logger.GetLogger("boulder").Errorf("Object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	part, err := _mps.UploadPart(body, &object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		UploadID:    uploadID,
		PartNumber:  int64(partNumber),
		AccessKeyID: accessKeyID,
		ContentLen:  contentLength,
		ContentMd5:  contentMd5,
	})
	if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidQueryParams)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidQueryParams)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchBucket)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchUpload)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchUpload)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if err != nil {
		logger.GetLogger("boulder").Errorf("upload %s : %d failed: %s", uploadID, partNumber, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	// 设置响应头
	w.Header().Set(xhttp.ETag, fmt.Sprintf(`"%s"`, part.ETag))
	w.WriteHeader(http.StatusOK)
}

// ListObjectPartsHandler 处理 ListParts请求  列出uploadid中已上传的分段
func ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: ListObjectPartsHandler")
	bucket, objectKey, _, accessKeyID := GetReqVar(r)
	// 解析查询参数
	query := r.URL.Query()
	uploadID := query.Get("uploadId")
	maxPartsStr := query.Get("max-parts")
	partNumberMarkerStr := query.Get("part-number-marker")

	// 解析 MaxParts
	maxParts := 1000 // 默认值
	if maxPartsStr != "" {
		parsedMaxParts, err := strconv.Atoi(maxPartsStr)
		if err != nil {
			logger.GetLogger("boulder").Errorf("invalid max-parts: %s", maxPartsStr)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}
		maxParts = parsedMaxParts
	}
	// 设置默认值
	if maxParts <= 0 || maxParts > 1000 {
		maxParts = 1000 // S3默认值和最大值
	}
	// 解析 PartNumberMarker
	partNumberMarker := 0 // 默认值
	if partNumberMarkerStr != "" {
		parsedMarker, err := strconv.Atoi(partNumberMarkerStr)
		if err != nil {
			logger.GetLogger("boulder").Errorf("invalid part-number-marker: %s", partNumberMarkerStr)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}
		partNumberMarker = parsedMarker
	}

	// 调用MultiPartService获取分段列表
	_mps := multipart.GetMultiPartService()
	if _mps == nil {
		logger.GetLogger("boulder").Errorf("multipart service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	// 调用ListParts方法
	upload, parts, err := _mps.ListParts(&object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		AccessKeyID: accessKeyID,
		UploadID:    uploadID,
		PartNumber:  int64(partNumberMarker),
		MaxParts:    int64(maxParts),
	})

	// 处理错误
	if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrAccessDenied)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchUpload)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchUpload)
		return
	}
	if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidQueryParams)) {
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidQueryParams)
		return
	}
	if err != nil {
		logger.GetLogger("boulder").Errorf("error listing object parts: %s", err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	resp := multipart.ListPartsResult{
		XMLName:          xml.Name{Local: "ListPartsResult"},
		XMLNS:            "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:           upload.Bucket,
		Key:              upload.Key,
		UploadId:         upload.UploadID,
		Initiator:        upload.Initiator,
		Owner:            upload.Owner,
		StorageClass:     upload.StorageClass,
		PartNumberMarker: partNumberMarker,
		MaxParts:         maxParts,
	}
	if len(parts) > maxParts {
		resp.IsTruncated = true
		resp.NextPartNumberMarker = parts[maxParts-1].PartNumber
		parts = parts[:maxParts]
	}
	for _, p := range parts {
		partInfo := meta.PartInfo{
			PartNumber:   p.PartNumber,
			ETag:         p.ETag,
			Size:         p.Size,
			LastModified: p.LastModified,
		}
		resp.Part = append(resp.Part, &partInfo)
	}

	xhttp.WriteAWSSuc(w, r, resp)
}
