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
	"strconv"

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
	ifnoneMatch := r.Header.Get(xhttp.IfNoneMatch)

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
		ETag:     fmt.Sprintf(`"%s"`, obj.ETag),
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

// AbortMultipartUploadHandler 处理 Abort Multipart Upload 请求
func AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: AbortMultipartUploadHandler")
	// TODO: 实现 Abort Multipart Upload 逻辑
	w.WriteHeader(http.StatusOK)
}

// ListMultipartUploadsHandler 处理 List Multipart Uploads 请求
func ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Infof("API called: ListMultipartUploadsHandler")
	// TODO: 实现 List Multipart Uploads 逻辑
	w.WriteHeader(http.StatusOK)
}

// DeleteMultipleObjectsHandler 处理 Delete Multiple Objects 请求
func DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Infof("API called: DeleteMultipleObjectsHandler")
	// TODO: 实现 Delete Multiple Objects 逻辑
	w.WriteHeader(http.StatusOK)
}

// CopyObjectPartHandler 处理 COPY Object Part 请求
func CopyObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Infof("API called: CopyObjectPartHandler")
	// TODO: 实现 COPY Object Part 逻辑
	w.WriteHeader(http.StatusOK)
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
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidPartNumber)
		return
	}
	contentLength := r.ContentLength
	if contentLength <= 0 {
		logger.GetLogger("boulder").Errorf("Invalid content length: %d", contentLength)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}
	// content Md5
	contentMd5 := r.Header.Get(xhttp.ContentMD5)

	_mps := multipart.GetMultiPartService()
	if _mps == nil {
		logger.GetLogger("boulder").Errorf("Object service not initialized")
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	part, err := _mps.UploadPart(r.Body, &object.BaseObjectParams{
		BucketName:  bucket,
		ObjKey:      objectKey,
		UploadID:    uploadID,
		PartNumber:  int64(partNumber),
		AccessKeyID: accessKeyID,
		ContentLen:  contentLength,
		ContentMd5:  contentMd5,
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("upload %s : %d failed: %s", uploadID, partNumber, err)
		xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
		return
	}

	// 设置响应头
	w.Header().Set(xhttp.ETag, fmt.Sprintf(`"%s"`, part.ETag))
	w.WriteHeader(http.StatusOK)
}

// ListObjectPartsHandler 处理 List Object Parts 请求
func ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("boulder").Infof("API called: ListObjectPartsHandler")
	// TODO: 实现 List Object Parts 逻辑
	w.WriteHeader(http.StatusOK)
}
