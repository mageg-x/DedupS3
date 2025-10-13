package handler

import (
	"archive/zip"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	xconf "github.com/mageg-x/dedups3/internal/config"
	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/utils"
	"github.com/mageg-x/dedups3/internal/vfs"
	"github.com/mageg-x/dedups3/meta"
	"github.com/mageg-x/dedups3/middleware"
	block2 "github.com/mageg-x/dedups3/plugs/block"
	"github.com/mageg-x/dedups3/plugs/kv"
	sb "github.com/mageg-x/dedups3/service/bucket"
	iam2 "github.com/mageg-x/dedups3/service/iam"
	"github.com/mageg-x/dedups3/service/object"
	"github.com/mageg-x/dedups3/service/stats"
	"github.com/mageg-x/dedups3/service/storage"
)

type PrepareEnv struct {
	username  string
	accountID string
	accessKey string
	ac        *meta.IamAccount
	user      *meta.IamUser
	iam       *iam2.IamService
	bs        *sb.BucketService
	os        *object.ObjectService
	bi        *meta.BucketMetadata
}

// IamUserInfo 构建返回的用户信息
type IamUserInfo struct {
	Username       string           `json:"username"`
	Account        string           `json:"account"`
	Group          []string         `json:"group"`
	Role           []string         `json:"role"`
	AttachPolicies []string         `json:"attachPolicies"`
	AllPolicies    []meta.Statement `json:"allPolicies"`
	Enabled        bool             `json:"enabled"`
	CreatedAt      time.Time        `json:"createdAt"`
}

func Prepare4Iam(w http.ResponseWriter, r *http.Request) *PrepareEnv {
	// 获取用户名信息
	username, _ := r.Context().Value("username").(string)
	if username == "" {
		xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "invalid username", nil, http.StatusUnauthorized)
		return nil
	}

	account, _ := r.Context().Value("account").(string)
	if account == "" {
		xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "invalid account", nil, http.StatusUnauthorized)
		return nil
	}

	// 获取访问密钥
	accountID := meta.GenerateAccountID(account)
	iamService := iam2.GetIamService()
	if iamService == nil {
		logger.GetLogger("dedups3").Errorf("failed to get iam service")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "service unavailable", nil, http.StatusServiceUnavailable)
		return nil
	}

	ac, err := iamService.GetAccount(accountID)
	if err != nil || ac == nil {
		logger.GetLogger("dedups3").Errorf("failed to get account %s", accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "account not found", nil, http.StatusForbidden)
		return nil
	}

	user, err := iamService.GetUser(accountID, username, username)
	if err != nil || user == nil || len(user.AccessKeys) == 0 {
		logger.GetLogger("dedups3").Errorf("failed to get root user for account %s", accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "service unavailable", nil, http.StatusServiceUnavailable)
		return nil
	}

	accessKeyIDs := utils.MapKeys(user.AccessKeys)
	logger.GetLogger("dedups3").Debug("account %s  user %s has access key %s", accountID, username, accessKeyIDs[0])
	return &PrepareEnv{
		username:  username,
		accountID: accountID,
		accessKey: accessKeyIDs[0],
		ac:        ac,
		user:      user,
		iam:       iamService,
	}
}

func Prepare4S3(w http.ResponseWriter, r *http.Request, bucketName string) *PrepareEnv {
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return nil
	}
	// 先检查bucket是否存在
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("dedups3").Errorf("bucket service not initialized")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "service unavailable", nil, http.StatusServiceUnavailable)
		return nil
	}
	bi, err := bs.GetBucketInfo(&sb.BaseBucketParams{
		BucketName:  bucketName,
		AccessKeyID: pe.accessKey,
	})
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "AccessDenied", nil, http.StatusForbidden)
			return nil
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "NoSuchBucket", nil, http.StatusNotFound)
			return nil
		}

		logger.GetLogger("dedups3").Errorf("get  bucket %s info failed: %v", bucketName, err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to upload file", nil, http.StatusInternalServerError)
		return nil
	}

	// 获取对象服务
	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("dedups3").Errorf("object service not initialized")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "service unavailable", nil, http.StatusServiceUnavailable)
		return nil
	}

	pe.os = _os
	pe.bs = bs
	pe.bi = bi
	return pe
}

func AdminLoginHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AccessKeyId     string `json:"accessKeyId,omitempty"`
		SecretAccessKey string `json:"secretAccessKey,omitempty"`
		Username        string `json:"username,omitempty"`
		Password        string `json:"password,omitempty"`
		Remember        bool   `json:"remember,omitempty"`
		Endpoint        string `json:"endpoint,omitempty"`
		Region          string `json:"region,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.GetLogger("dedups3").Errorf("failed decoding request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid params", nil, http.StatusBadRequest)
		return
	}

	loginname := strings.TrimSpace(req.Username)
	username, account := middleware.ParseLoginUsername(loginname)
	if err := meta.ValidateUsername(username); err != nil {
		logger.GetLogger("dedups3").Errorf("username %s is invalid format", username)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid username", nil, http.StatusBadRequest)
		return
	}
	accountID := meta.GenerateAccountID(account)

	iam := iam2.GetIamService()
	_, err := iam.GetAccount(accountID)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get account %s", req.Username)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "failed to get account", nil, http.StatusBadRequest)
		return
	}
	// 验证用户名密码
	user, err := iam.GetUser(accountID, username, username)
	if user == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("user %s not found", username)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "user not found", nil, http.StatusBadRequest)
		return
	}

	password := strings.TrimSpace(req.Password)
	inputStr := user.Password + ":" + loginname
	outputStr := md5.Sum([]byte(inputStr))
	expectStr := hex.EncodeToString(outputStr[:])
	if expectStr != password {
		logger.GetLogger("dedups3").Errorf("password mismatch for user %s, %s:%s", username, password, expectStr)
		xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "passwords mismatch", nil, http.StatusUnauthorized)
		return
	}

	// 生成 JWT
	token, err := utils.GenerateToken(loginname)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to generate token for user %s", username)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "internal server error", nil, http.StatusInternalServerError)
		return
	}

	sameSite := http.SameSiteStrictMode
	if xconf.IsDev {
		sameSite = http.SameSiteLaxMode
	}
	// 设置 Cookie
	cookie := &http.Cookie{
		Name:     "access_token", // Cookie 名字
		Value:    token,          // Token 值
		Path:     "/",            // 作用路径
		HttpOnly: true,           // 关键：JS 无法读取，防 XSS
		Secure:   !xconf.IsDev,   // 仅 HTTPS（开发环境可设 false）
		SameSite: sameSite,       // 防 CSRF
		MaxAge:   3600,           // 有效期 1 小时（秒）
		// Expires: time.Now().Add(1 * time.Hour), // 过期时间（旧方式）
	}
	http.SetCookie(w, cookie)

	w.Header().Set("Content-Type", "application/json")

	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminLogoutHandler(w http.ResponseWriter, r *http.Request) {
	sameSite := http.SameSiteStrictMode
	if xconf.IsDev {
		sameSite = http.SameSiteLaxMode
	}

	// 清除 Cookie：设置 Max-Age=-1 或空值
	http.SetCookie(w, &http.Cookie{
		Name:     "access_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   !xconf.IsDev, // 生产环境必须为 true
		SameSite: sameSite,
		MaxAge:   -1, // 表示立即过期
	})

	w.Header().Set("Content-Type", "application/json")
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminGetStatsHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call GetStatsHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	ss := stats.GetStatsService()
	_stats, err := ss.GetAccountStats(pe.accountID)
	if err != nil || _stats == nil {
		logger.GetLogger("dedups3").Errorf("failed to get stats for account %s", pe.username)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "internal server error", nil, http.StatusInternalServerError)
		return
	}
	ss.RefreshAccountStats(pe.accountID)

	xhttp.AdminWriteJSONError(w, r, 0, "success", _stats, http.StatusOK)
}

func AdminListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call ListBucketHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("dedups3").Errorf("bucket service is nil")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}

	buckets, _, err := bs.ListBuckets(&sb.BaseBucketParams{
		AccessKeyID: pe.accessKey,
	})
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to list buckets: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "failed to list buckets", nil, http.StatusServiceUnavailable)
	}

	ss := stats.GetStatsService()
	type Bucket struct {
		Base  *meta.BucketMetadata `json:"base"`
		Stats *stats.StatsOfBucket `json:"stats,omitempty"`
	}
	bucketList := make([]Bucket, 0, len(buckets))
	needRefresh := false
	for _, _bucket := range buckets {
		if _bucket != nil {
			bucket := Bucket{
				Base: _bucket,
			}
			if _stats, err := ss.GetBucketStats(_bucket.Name); err == nil && _stats != nil {
				bucket.Stats = _stats
			} else if err != nil && strings.Contains(err.Error(), "not found") {
				logger.GetLogger("dedups3").Errorf("get bucket %s stats not found", _bucket.Name)
				needRefresh = true
			}
			bucketList = append(bucketList, bucket)
		}
	}
	if needRefresh {
		ss.RefreshAccountStats(pe.accountID)
	}
	xhttp.AdminWriteJSONError(w, r, 0, "success", bucketList, http.StatusOK)
}

func AdminCreateBucketHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call createbuckethandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	// 定义接收结构体
	var req struct {
		Name   string `json:"name"`
		Region string `json:"region"`
	}

	// 解析 JSON body
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("dedups3").Errorf("bucket service is nil")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}
	cfg := xconf.Get()
	err := bs.CreateBucket(&sb.BaseBucketParams{
		BucketName:        req.Name,
		Location:          cfg.Node.Region,
		ObjectLockEnabled: false,
		AccessKeyID:       pe.accessKey,
	})
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed create bucket err: %v", err)

		if errors.Is(err, xhttp.ToError(xhttp.ErrBucketAlreadyOwnedByYou)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusConflict, "BucketAlreadyOwnedByYou", nil, http.StatusConflict)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrBucketAlreadyExists)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusConflict, "BucketAlreadyExists", nil, http.StatusConflict)
		} else {
			xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "BucketMetadataNotInitialized", nil, http.StatusServiceUnavailable)
		}
		return
	}

	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminDeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call deletebuckethandler] %#v", r.URL)
	query := utils.DecodeQuerys(r.URL.Query())
	bucketName := query.Get("name")

	pe := Prepare4S3(w, r, bucketName)
	if pe == nil {
		return
	}

	err := pe.bs.DeleteBucket(&sb.BaseBucketParams{
		BucketName:  bucketName,
		AccessKeyID: pe.accessKey,
	})

	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed create bucket err: %v", err)

		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "NoSuchBucket", nil, http.StatusNotFound)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrBucketNotEmpty)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusConflict, "BucketNotEmpty", nil, http.StatusConflict)
		} else if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "AccessDenied", nil, http.StatusForbidden)
		} else {
			xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "BucketMetadataNotInitialized", nil, http.StatusServiceUnavailable)
		}
		return
	}

	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminListObjectsHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminlistobjectshandler] %#v", r.URL)

	query := utils.DecodeQuerys(r.URL.Query())
	bucketName := query.Get("bucket")
	bucketName = strings.TrimSpace(bucketName)
	prefix := query.Get("prefix")
	prefix = strings.TrimSpace(prefix)
	// 将多个连续的 / 替换成一个 /
	prefix = regexp.MustCompile("/+").ReplaceAllString(prefix, "/")

	marker := query.Get("marker")
	delimiter := "/"
	logger.GetLogger("dedups3").Debugf("get query %#v  prefix %s", query, prefix)

	pe := Prepare4S3(w, r, bucketName)
	if pe == nil {
		return
	}

	objects, commonPrefixes, isTruncated, nextMarker, err := pe.os.ListObjects(bucketName, pe.accessKey, prefix, marker, delimiter, 1000)
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "AccessDenied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "NoSuchBucket", nil, http.StatusNotFound)
			return
		}

		logger.GetLogger("dedups3").Errorf("error listing objects: %s", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "internal server error", nil, http.StatusInternalServerError)
		return
	}

	logger.GetLogger("dedups3").Infof("bucket %s listobjects: files %d, folders %d  nextmarker %s has more %v",
		bucketName, len(objects), len(commonPrefixes), nextMarker, isTruncated)

	type ObjectItem struct {
		IsFolder   bool              `json:"isFolder"`
		CreatedAt  *time.Time        `json:"createdAt,omitempty"`
		LastModify *time.Time        `json:"lastModify,omitempty"`
		Name       string            `json:"name"`
		Size       int64             `json:"size,omitempty"`
		Etag       string            `json:"etag,omitempty"`
		Tags       map[string]string `json:"tags,omitempty"`
		Chunks     int32             `json:"chunks,omitempty"`
	}

	type ObjectList struct {
		Total      int64         `json:"total"`
		Objects    []*ObjectItem `json:"objects,omitempty"`
		NextMarker string        `json:"nextMarker"`
	}

	num := int64(len(objects) + len(commonPrefixes))
	resp := &ObjectList{Total: num, NextMarker: nextMarker, Objects: make([]*ObjectItem, 0, num)}
	for _, o := range objects {
		if len(o.Key) == 0 || strings.HasSuffix(o.Key, "/") {
			// 过滤掉一些目录文件
			continue
		}
		item := ObjectItem{
			IsFolder:   false,
			CreatedAt:  &o.CreatedAt,
			LastModify: &o.LastModified,
			Name:       o.Key,
			Size:       o.Size,
			Etag:       string(o.ETag),
			Tags:       o.Tags,
			Chunks:     int32(len(o.Chunks)),
		}
		resp.Objects = append(resp.Objects, &item)
	}

	for _, o := range commonPrefixes {
		item := ObjectItem{
			IsFolder: true,
			Name:     o,
		}
		resp.Objects = append(resp.Objects, &item)
	}

	xhttp.AdminWriteJSONError(w, r, 0, "success", resp, http.StatusOK)
}

func AdminCreateFolderHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call admincreatefolderhandler] %#v", r.URL)

	// 定义接收结构体
	var req struct {
		BucketName string `json:"bucket"`
		FolderName string `json:"folder"`
	}

	// 解析 JSON body
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	req.BucketName = strings.TrimSpace(req.BucketName)
	req.FolderName = strings.TrimSpace(req.FolderName)

	if req.BucketName == "" || req.FolderName == "" {
		logger.GetLogger("dedups3").Errorf("invalid bucket or folder")
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid bucket or folder", nil, http.StatusBadRequest)
		return
	}

	pe := Prepare4S3(w, r, req.BucketName)
	if pe == nil {
		return
	}

	var body io.Reader = strings.NewReader("")
	_, err := pe.os.PutObject(body, r.Header, &object.BaseObjectParams{
		BucketName:  req.BucketName,
		ObjKey:      req.FolderName,
		IfNoneMatch: "*",
		AccessKeyID: pe.accessKey,
	})

	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "AccessDenied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrPreconditionFailed)) {
			// 目录已经存在
			xhttp.AdminWriteJSONError(w, r, http.StatusConflict, "AlreadyExists", nil, http.StatusConflict)
			return
		}

		logger.GetLogger("dedups3").Errorf("error create folder objects: %s", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "internal server error", nil, http.StatusInternalServerError)
		return
	}

	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminPutObjectHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminputobjecthandler]")
	// 限制请求体大小
	const maxRequestSize = 500 << 20 // 500MB
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)
	// 设置一个较小的内存限制，大文件会自动写入临时文件
	const maxMemory = 10 << 20 // 10MB
	err := r.ParseMultipartForm(maxMemory)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to parse multipart form: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "failed to parse form", nil, http.StatusBadRequest)
		return
	}

	// 清理临时文件
	defer func() {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
	}()

	// 获取表单字段
	bucketName := r.FormValue("bucket")
	objectName := r.FormValue("object")
	contentType := r.FormValue("contentType")

	bucketName = strings.TrimSpace(bucketName)
	objectName = strings.TrimSpace(objectName)
	objectName = path.Clean(objectName)

	logger.GetLogger("dedups3").Debugf(" bucket: %s, object: %s, contenttype: %s", bucketName, objectName, contentType)
	if err := utils.CheckValidObjectName(objectName); err != nil || bucketName == "" || objectName == "" {
		logger.GetLogger("dedups3").Errorf("invalid bucket or object name")
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid bucket or object name", nil, http.StatusBadRequest)
		return
	}

	pe := Prepare4S3(w, r, bucketName)
	if pe == nil {
		return
	}

	// 打开上传的文件
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to get file from form: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "missing file", nil, http.StatusBadRequest)
		return
	}
	defer file.Close()

	if contentType == "" {
		contentType = fileHeader.Header.Get("Content-Type")
	}

	// 记录文件信息
	logger.GetLogger("dedups3").Infof("uploading file: %s, size: %d bytes", fileHeader.Filename, fileHeader.Size)

	// 创建请求头的副本
	head := make(http.Header)
	for k, v := range r.Header {
		head[k] = v
	}
	// 设置Content-Type
	if contentType != "" {
		head.Set("Content-Type", contentType)
	}

	// 调用PutObject方法上传文件（流式处理）
	obj, err := pe.os.PutObject(file, head, &object.BaseObjectParams{
		BucketName:  bucketName,
		ObjKey:      objectName,
		AccessKeyID: pe.accessKey,
		ContentType: contentType,
	})

	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAdminBucketQuotaExceeded)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "QuotaExceeded", nil, http.StatusBadRequest)
			return
		}

		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "AccessDenied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "NoSuchBucket", nil, http.StatusNotFound)
			return
		}

		logger.GetLogger("dedups3").Errorf("error uploading object: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to upload file", nil, http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"bucket": bucketName,
		"key":    objectName,
		"etag":   obj.ETag,
		"size":   obj.Size,
	}
	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", resp, http.StatusOK)
}

func AdminDelObjectHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call admindelobjecthandler]")

	type Req struct {
		BucketName string   `json:"bucket"`
		Keys       []string `json:"keys"` // 对象键或目录路径列表
	}
	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}
	logger.GetLogger("dedups3").Debug("get delete request object list : %#v", req.Keys)

	// 验证参数
	req.BucketName = strings.TrimSpace(req.BucketName)
	if req.BucketName == "" || len(req.Keys) == 0 {
		logger.GetLogger("dedups3").Errorf("invalid bucket name or empty keys list")
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid bucket name or empty keys list", nil, http.StatusBadRequest)
		return
	}

	pe := Prepare4S3(w, r, req.BucketName)
	if pe == nil {
		return
	}

	// 收集所有需要删除的对象键
	var allObjectKeys []string

	// 处理每个输入的键
	for _, key := range req.Keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}

		// 判断是否为目录（以'/'结尾）
		if strings.HasSuffix(key, "/") {
			// 将多个连续的 / 替换成一个 /
			key = regexp.MustCompile("/+").ReplaceAllString(key, "/")
			// 列出目录下的对象和子目录
			marker := ""
			for {
				objects, _, isTruncated, nextMarker, err := pe.os.ListObjects(req.BucketName, pe.accessKey, key, marker, "", 1000)
				if err != nil {
					logger.GetLogger("dedups3").Errorf("error listing objects of folder %s : %v", key, err)
					xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "object service enum folder failed", nil, http.StatusInternalServerError)
					return
				}
				for _, o := range objects {
					allObjectKeys = append(allObjectKeys, o.Key)
				}
				if !isTruncated {
					break
				}
				marker = nextMarker
			}
		} else {
			// 单个对象直接添加到列表
			allObjectKeys = append(allObjectKeys, key)
		}
	}
	logger.GetLogger("dedups3").Debugf("Prepare4S3 to delete key %#v", allObjectKeys)
	deleteKeys := make([]string, 0, len(allObjectKeys))
	for _, objkey := range allObjectKeys {
		// 执行删除操作
		err := pe.os.DeleteObject(&object.BaseObjectParams{
			BucketName:  req.BucketName,
			ObjKey:      objkey,
			AccessKeyID: pe.accessKey,
		})
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to delete object %s: %v", objkey, err)
		} else {
			deleteKeys = append(deleteKeys, objkey)
		}
	}
	// 返回成功响应
	resp := map[string]interface{}{
		"bucket":  req.BucketName,
		"deleted": len(deleteKeys),
	}

	logger.GetLogger("dedups3").Infof("successfully deleted %d objects from bucket %s", len(deleteKeys), req.BucketName)
	xhttp.AdminWriteJSONError(w, r, 0, "success", resp, http.StatusOK)
}

func AdminGetObjectHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call admingetobjecthandler] %#v", r.URL)
	type Req struct {
		BucketName string   `json:"bucket"`
		Files      []string `json:"files"`
		Filename   string   `json:"filename"`
	}
	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Files) == 0 {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	objectKeys := make([]string, 0, len(req.Files))
	for _, filename := range req.Files {
		filename = strings.TrimSpace(filename)
		if filename == "" {
			continue
		}
		hadTrailingSlash := strings.HasSuffix(filename, "/")
		filename = path.Clean(filename)
		if hadTrailingSlash {
			filename += "/"
		}
		filename = regexp.MustCompile("/+").ReplaceAllString(filename, "/")
		if filename != "" {
			objectKeys = append(objectKeys, filename)
		}
	}

	req.BucketName = strings.TrimSpace(req.BucketName)
	req.Filename = strings.TrimSpace(req.Filename)
	if req.Filename == "" {
		if len(objectKeys) == 1 {
			req.Filename = path.Base(objectKeys[0])
		} else {
			req.Filename = "download.zip"
		}
	}
	logger.GetLogger("dedups3").Errorf("download object list : %#v", objectKeys)

	pe := Prepare4S3(w, r, req.BucketName)
	if pe == nil {
		return
	}

	// 收集所有需要下载的对象键
	var allObjectKeys []string

	// 处理每个输入的键
	for _, key := range objectKeys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}

		// 判断是否为目录（以'/'结尾）
		if strings.HasSuffix(key, "/") {
			// 将多个连续的 / 替换成一个 /
			key = regexp.MustCompile("/+").ReplaceAllString(key, "/")
			// 列出目录下的对象和子目录
			marker := ""
			for {
				objects, _, isTruncated, nextMarker, err := pe.os.ListObjects(req.BucketName, pe.accessKey, key, marker, "", 1000)
				if err != nil {
					logger.GetLogger("dedups3").Errorf("error listing objects of folder %s : %v", key, err)
					xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "object service enum folder failed", nil, http.StatusInternalServerError)
					return
				}
				for _, o := range objects {
					allObjectKeys = append(allObjectKeys, o.Key)
				}
				if !isTruncated {
					break
				}
				marker = nextMarker
			}
		} else {
			// 单个对象直接添加到列表
			allObjectKeys = append(allObjectKeys, key)
		}
	}
	logger.GetLogger("dedups3").Errorf("Prepare4S3 to download key %#v", allObjectKeys)
	if len(allObjectKeys) == 0 {
		logger.GetLogger("dedups3").Errorf("no object to download")
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	if len(allObjectKeys) == 1 {
		// 打开对象读取器
		obj, objReader, err := pe.os.GetObject(nil, nil, &object.BaseObjectParams{
			BucketName:  req.BucketName,
			ObjKey:      allObjectKeys[0],
			AccessKeyID: pe.accessKey,
		})
		if err != nil {
			if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
				xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "AccessDenied", nil, http.StatusForbidden)
				return
			}
			if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
				xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "NoSuchKey", nil, http.StatusNotFound)
				return
			}

			logger.GetLogger("dedups3").Errorf("failed to get object %s: %v", allObjectKeys[0], err)
			xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "internal server error", nil, http.StatusInternalServerError)
			return
		}
		defer objReader.Close()

		// 设置响应头
		w.Header().Set(xhttp.ContentType, obj.ContentType)
		w.Header().Set(xhttp.ContentLength, fmt.Sprintf("%d", obj.Size))
		w.Header().Set(xhttp.ETag, string(obj.ETag))
		if obj.ContentEncoding != "" {
			w.Header().Set(xhttp.ContentEncoding, obj.ContentEncoding)
		}
		if obj.ContentLanguage != "" {
			w.Header().Set(xhttp.ContentLanguage, obj.ContentLanguage)
		}
		if obj.ContentDisposition != "" {
			w.Header().Set(xhttp.ContentDisposition, obj.ContentDisposition)
		}
		// 流式输出：防止大文件 OOM
		_, err = io.Copy(w, objReader)
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

		logger.GetLogger("dedups3").Errorf("successfully downloaded object %s from bucket %s", allObjectKeys[0], req.BucketName)
	} else {
		// 多个对象或目录，打包成zip文件
		// 设置响应头
		w.Header().Set("Content-Type", "application/zip")
		filenameHeader := fmt.Sprintf("attachment; filename*=%s", url.QueryEscape(req.Filename))
		w.Header().Set("Content-Disposition", filenameHeader) // 创建zip写入器，直接写入响应流
		zipWriter := zip.NewWriter(w)
		defer zipWriter.Close()
		// 遍历所有对象，添加到zip文件
		for _, key := range allObjectKeys {
			// 打开对象读取器
			_, objReader, err := pe.os.GetObject(nil, nil, &object.BaseObjectParams{
				BucketName:  req.BucketName,
				ObjKey:      key,
				AccessKeyID: pe.accessKey,
			})
			if err == nil {
				defer objReader.Close() // 更早关闭
				fw, _ := zipWriter.Create(key)
				_, err := io.Copy(fw, objReader)
				if err != nil {
					logger.GetLogger("dedups3").Errorf("copy object %s to zip failed: %v", key, err)
				}
			} else {
				logger.GetLogger("dedups3").Errorf("failed to get object %s: %v", key, err)
			}
		}

		// 确保zip写入器被刷新
		if err := zipWriter.Flush(); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to flush zip writer: %v", err)
		}

		logger.GetLogger("dedups3").Infof("successfully downloaded %d objects as zip from bucket %s", len(allObjectKeys), req.BucketName)
	}
}

func AdminListUserHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[call adminListUserHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	userList := make([]*IamUserInfo, 0)

	for uname, _ := range pe.ac.Users {
		u, err := pe.iam.GetUser(pe.accountID, pe.username, uname)
		if err != nil || u == nil {
			logger.GetLogger("dedups3").Errorf("failed to get user %s: %v", uname, err)
			continue
		}
		if u.IsRoot {
			continue
		}
		ui := &IamUserInfo{
			Username:       u.Username,
			Account:        pe.ac.Name,
			Group:          utils.StringSlice(utils.MapKeys(u.Groups)),
			Role:           utils.StringSlice(utils.MapKeys(u.Roles)),
			AttachPolicies: utils.StringSlice(utils.MapKeys(u.AttachedPolicies)),
			AllPolicies:    make([]meta.Statement, 0),
			Enabled:        u.Enabled,
			CreatedAt:      u.CreatedAt,
		}
		userList = append(userList, ui)
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", userList, http.StatusOK)
}
func AdminGetUserHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[call adminGetUserHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	userInfo := &IamUserInfo{
		Username:       pe.username,
		Account:        pe.ac.Name,
		Group:          utils.StringSlice(utils.MapKeys(pe.user.Groups)),
		Role:           utils.StringSlice(utils.MapKeys(pe.user.Roles)),
		AttachPolicies: utils.StringSlice(utils.MapKeys(pe.user.AttachedPolicies)),
		AllPolicies:    make([]meta.Statement, 0),
		Enabled:        pe.user.Enabled,
		CreatedAt:      pe.user.CreatedAt,
	}

	allPolices, _ := pe.iam.ListUserAllPolicies(pe.accountID, pe.username)
	policyFilter := make(map[string]bool)
	for _, policy := range allPolices {
		if policy == nil {
			continue
		}
		if policyFilter[policy.Name] {
			continue
		} else {
			policyFilter[policy.Name] = true
		}
		if policy.Document != "" {
			var pd meta.PolicyDocument
			err := json.Unmarshal([]byte(policy.Document), &pd)
			if err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal policy %s: %v", policy.Name, err)
				continue
			}
			if pd.Statement != nil {
				userInfo.AllPolicies = append(userInfo.AllPolicies, pd.Statement...)
			}
		}
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", userInfo, http.StatusOK)
}

func AdminCreateUserHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminCreateUserHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	type Req struct {
		Username       string   `json:"username"`
		Password       string   `json:"password,omitempty"`
		Groups         []string `json:"groups,omitempty"`
		Roles          []string `json:"roles,omitempty"`
		AttachPolicies []string `json:"attachPolicies,omitempty"`
		Enabled        bool     `json:"enabled"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Username == "" || req.Password == "" {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	if pe.ac == nil || pe.user == nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "invalid create user request", nil, http.StatusInternalServerError)
		return
	}

	_, err := pe.iam.CreateUser(pe.accountID, pe.username, req.Username, req.Password, req.Groups, req.Roles, req.AttachPolicies, req.Enabled)
	if err != nil {
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		logger.GetLogger("dedups3").Errorf("failed to create policy: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidName)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid name", nil, http.StatusBadRequest)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrUserAlreadyExists)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusConflict, "user already exists", nil, http.StatusConflict)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid user document", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminUpdateUserHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminUpdateUserHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	type Req struct {
		Username       string   `json:"username"`
		Password       string   `json:"password,omitempty"`
		Groups         []string `json:"groups,omitempty"`
		Roles          []string `json:"roles,omitempty"`
		AttachPolicies []string `json:"attachPolicies,omitempty"`
		Enabled        bool     `json:"enabled"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Username == "" {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	if pe.ac == nil || pe.user == nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "invalid create user request", nil, http.StatusInternalServerError)
		return
	}

	_, err := pe.iam.UpdateUser(pe.accountID, pe.username, req.Username, req.Password, req.Groups, req.Roles, req.AttachPolicies, req.Enabled)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to create user: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidName)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid name", nil, http.StatusBadRequest)
			return
		}

		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrAdminNoSuchUser)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "user not exists", nil, http.StatusNotFound)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid user document", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminDeleteUserHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminDeleteUserHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}
	if pe.ac == nil || pe.user == nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "invalid policy request", nil, http.StatusInternalServerError)
		return
	}

	query := utils.DecodeQuerys(r.URL.Query())
	username := query.Get("username")
	username = strings.TrimSpace(username)

	err := pe.iam.DeleteUser(pe.accountID, pe.username, username)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete user: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrAdminNoSuchUser)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "user not exists", nil, http.StatusNotFound)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "delete user failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminListPolicyHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminListPolicyHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}
	policies := make([]*meta.IamPolicy, 0)
	if pe.ac.Policies != nil {
		for pname, _ := range pe.ac.Policies {
			policy, err := pe.iam.GetPolicy(pe.accountID, pe.username, pname)
			if err != nil || policy == nil {
				logger.GetLogger("dedups3").Errorf("failed to get policy: %v", err)
				xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to get policy", nil, http.StatusInternalServerError)
				return
			}
			if policy.Name != "root-policy" {
				policies = append(policies, policy)
			}
		}
	}
	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", policies, http.StatusOK)
}

func AdminGetPolicyHandler(w http.ResponseWriter, r *http.Request) {
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}
	if pe.ac == nil || pe.ac.Policies == nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "invalid policy request", nil, http.StatusInternalServerError)
		return
	}

	query := utils.DecodeQuerys(r.URL.Query())
	name := query.Get("name")
	name = strings.TrimSpace(name)

	if _, exists := pe.ac.Policies[name]; !exists {
		logger.GetLogger("dedups3").Errorf("policy %s does not exist", name)
		xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "policy name does not exist", nil, http.StatusNotFound)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", pe.ac.Policies[name], http.StatusOK)
}

func AdminUpdatePolicyHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminListPolicyHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	type Req struct {
		Name string `json:"name"`
		Desc string `json:"desc"`
		Doc  string `json:"doc"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}
	if pe.ac == nil || pe.user == nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "invalid policy document", nil, http.StatusInternalServerError)
		return
	}

	_, err := pe.iam.UpdatePolicy(pe.accountID, pe.username, req.Name, req.Desc, req.Doc)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to update policy: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchIamPolicy)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "policy not exists", nil, http.StatusNotFound)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid policy document", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminCreatePolicyHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminListPolicyHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	if pe.ac == nil || pe.user == nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "invalid policy document", nil, http.StatusInternalServerError)
		return
	}
	type Req struct {
		Name string `json:"name"`
		Desc string `json:"desc"`
		Doc  string `json:"doc"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}
	_, err := pe.iam.CreatePolicy(pe.accountID, pe.username, req.Name, req.Desc, req.Doc)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to create policy: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidName)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid name", nil, http.StatusBadRequest)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrPolicyAlreadyExists)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusConflict, "policy already exists", nil, http.StatusConflict)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid policy document", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminDeletePolicyHandler(w http.ResponseWriter, r *http.Request) {
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}
	if pe.ac == nil || pe.user == nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "invalid policy request", nil, http.StatusInternalServerError)
		return
	}

	query := utils.DecodeQuerys(r.URL.Query())
	name := query.Get("name")
	name = strings.TrimSpace(name)

	err := pe.iam.DeletePolicy(pe.accountID, pe.username, name)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete policy: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchIamPolicy)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "policy not exists", nil, http.StatusNotFound)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "delete policy failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminListRoleHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminListRoleHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	roles := make([]*meta.IamRole, 0)
	for rolename, _ := range pe.ac.Roles {
		if role, err := pe.iam.GetRole(pe.accountID, pe.username, rolename); err == nil && role != nil {
			roles = append(roles, role)
		}
	}
	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", roles, http.StatusOK)
}

func AdminGetRoleHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminGetRoleHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	query := utils.DecodeQuerys(r.URL.Query())
	name := query.Get("name")
	name = strings.TrimSpace(name)
	role, err := pe.iam.GetRole(pe.accountID, pe.username, name)
	if role == nil || err != nil {
		logger.GetLogger("dedups3").Errorf("role %s does not exist", name)
		xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "role name does not exist", nil, http.StatusNotFound)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", role, http.StatusOK)
}

func AdminCreateRoleHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminCreateRoleHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	type Req struct {
		Name           string   `json:"name"`
		Desc           string   `json:"desc"`
		AttachPolicies []string `json:"attachPolicies"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	_, err := pe.iam.CreateRole(pe.accountID, pe.username, req.Name, req.Desc, "", req.AttachPolicies)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to create role: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidName)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid name", nil, http.StatusBadRequest)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchIamPolicy)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "policy not exists", nil, http.StatusNotFound)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrRoleAlreadyExists)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusConflict, "role already exists", nil, http.StatusConflict)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "create role failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminUpdateRoleHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminUpdateRoleHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	type Req struct {
		Name           string   `json:"name"`
		Desc           string   `json:"desc"`
		AttachPolicies []string `json:"attachPolicies"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	_, err := pe.iam.UpdateRole(pe.accountID, pe.username, req.Name, req.Desc, "", req.AttachPolicies)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to update role: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchRole)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "role not exists", nil, http.StatusNotFound)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchIamPolicy)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "policy not exists", nil, http.StatusNotFound)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "create role failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminDeleteRoleHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminDeleteRoleHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	query := utils.DecodeQuerys(r.URL.Query())
	name := query.Get("name")
	name = strings.TrimSpace(name)

	if err := pe.iam.DeleteRole(pe.accountID, pe.username, name); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete role: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchRole)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "role not exists", nil, http.StatusNotFound)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "delete role failed", nil, http.StatusBadRequest)
		return
	}
	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminListGroupHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminListGroupHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}
	groups := make([]*meta.IamGroup, 0)
	if pe.ac.Groups != nil {
		for groupname, _ := range pe.ac.Groups {
			group, err := pe.iam.GetGroup(pe.accountID, pe.username, groupname)
			if err != nil || group == nil {
				logger.GetLogger("dedups3").Errorf("failed to list group: %v", err)
				xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to list group", nil, http.StatusInternalServerError)
				return
			}
			groups = append(groups, group)
		}
	}
	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", groups, http.StatusOK)
}

func AdminGetGroupHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminGetGroupHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	query := utils.DecodeQuerys(r.URL.Query())
	name := query.Get("name")
	name = strings.TrimSpace(name)

	group, err := pe.iam.GetGroup(pe.accountID, pe.username, name)
	if err != nil || group == nil {
		logger.GetLogger("dedups3").Errorf("group %s does not exist", name)
		xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "group name does not exist", nil, http.StatusNotFound)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", group, http.StatusOK)
}

func AdminCreateGroupHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminCreateGroupHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	type Req struct {
		Name           string   `json:"name"`
		Desc           string   `json:"desc"`
		Users          []string `json:"users"`
		AttachPolicies []string `json:"attachPolicies"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	_, err := pe.iam.CreateGroup(pe.accountID, pe.username, req.Name, req.Desc, req.Users, req.AttachPolicies)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to create group: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidName)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid name", nil, http.StatusBadRequest)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrGroupAlreadyExists)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusConflict, "group already exists", nil, http.StatusConflict)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "create role failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminUpdateGroupHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminUpdateGroupHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	type Req struct {
		Name           string   `json:"name"`
		Desc           string   `json:"desc"`
		Users          []string `json:"users"`
		AttachPolicies []string `json:"attachPolicies"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	_, err := pe.iam.UpdateGroup(pe.accountID, pe.username, req.Name, req.Desc, req.Users, req.AttachPolicies)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to create group: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchGroup)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "group not exists", nil, http.StatusNotFound)
			return
		}

		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "create role failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminDeleteGroupHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminDeleteGroupHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	query := utils.DecodeQuerys(r.URL.Query())
	name := query.Get("name")
	name = strings.TrimSpace(name)

	if err := pe.iam.DeleteGroup(pe.accountID, pe.username, name); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete group: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchGroup)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "group not exists", nil, http.StatusNotFound)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "delete group failed", nil, http.StatusBadRequest)
		return
	}
	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminListAccessKeyHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminListAccessKeyHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil || pe.user == nil {
		return
	}
	type AkItem struct {
		AccessKeyID     string    `json:"accessKeyId"`
		SecretAccessKey string    `json:"secretAccessKey"`
		Enable          bool      `json:"enable"`
		ExpiredAt       time.Time `json:"expiredAt"`
		Creater         string    `json:"creater"`
	}

	akList := make([]AkItem, 0)
	for username, _ := range pe.ac.Users {
		user, err := pe.iam.GetUser(pe.accountID, pe.username, username)
		if user == nil || err != nil {
			continue
		}
		for accessKeyID, _ := range user.AccessKeys {
			ak, err := pe.iam.GetAccessKey(accessKeyID)
			if ak == nil || err != nil {
				logger.GetLogger("dedups3").Errorf("failed to get access key: %v", err)
				xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to get access key", nil, http.StatusInternalServerError)
				return
			}

			akList = append(akList, AkItem{
				AccessKeyID:     ak.AccessKeyID,
				SecretAccessKey: ak.SecretAccessKey,
				Enable:          ak.Status,
				ExpiredAt:       ak.ExpiredAt,
				Creater:         user.Username,
			})
		}
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", &akList, http.StatusOK)
}

func AdminCreateAccessKeyHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminCreateAccessKeyHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	type Req struct {
		AccessKey string    `json:"accessKey"`
		SecretKey string    `json:"secretKey"`
		ExpiredAt time.Time `json:"expiredAt"`
		Enabled   bool      `json:"enabled"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AccessKey == "" || req.SecretKey == "" {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	if _, err := pe.iam.CreateAccessKey(pe.accountID, pe.username, req.AccessKey, req.SecretKey, req.ExpiredAt, req.Enabled); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to create accesskey: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidAccessKeyID)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid formate access key", nil, http.StatusBadRequest)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrAdminInvalidSecretKey)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid formate secret key", nil, http.StatusBadRequest)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrAdminConfigDuplicateKeys)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusConflict, "accessKey already exists", nil, http.StatusConflict)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "create accessKey failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminUpdateAccessKeyHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call AdminUpdateAccessKeyHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	type Req struct {
		AccessKey string    `json:"accessKey"`
		SecretKey string    `json:"secretKey"`
		ExpiredAt time.Time `json:"expiredAt"`
		Enabled   bool      `json:"enabled"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AccessKey == "" || req.SecretKey == "" {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	if _, err := pe.iam.UpdateAccessKey(pe.accountID, req.AccessKey, req.SecretKey, req.ExpiredAt, req.Enabled); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to create access key: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrInvalidAccessKeyID)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid formate key", nil, http.StatusBadRequest)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrAdminInvalidSecretKey)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid formate key", nil, http.StatusBadRequest)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrAdminNoSuchAccessKey)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "accessKey not exists", nil, http.StatusNotFound)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "create accessKey failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminDeleteAccessKeyHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminDeleteAccessKeyHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	query := utils.DecodeQuerys(r.URL.Query())
	accessKey := query.Get("accessKey")
	accessKey = strings.TrimSpace(accessKey)

	if err := pe.iam.DeleteAccessKey(pe.accountID, accessKey); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete accessKey: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrAdminNoSuchAccessKey)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "accessKey not exists", nil, http.StatusNotFound)
			return
		}
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "delete accessKey failed", nil, http.StatusBadRequest)
		return
	}
	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminListQuotaHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminListQuotaHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	logger.GetLogger("dedups3").Errorf("quota list  %#v", pe.ac.Quota)

	type Resp struct {
		AccountID      string `json:"accountID"`
		MaxSpaceSize   int    `json:"maxSpaceSize"`
		MaxObjectCount int    `json:"maxObjectCount"`
		Enable         bool   `json:"enable"`
	}

	quotaList := make([]*Resp, 0)
	if pe.ac.Quota != nil {
		quotaList = append(quotaList, &Resp{
			AccountID:      pe.ac.Name,
			MaxSpaceSize:   pe.ac.Quota.MaxSpaceSize,
			MaxObjectCount: pe.ac.Quota.MaxObjectCount,
			Enable:         pe.ac.Quota.Enable,
		})
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", quotaList, http.StatusOK)
}

func AdminCreateQuotaHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminCreateQuotaHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}

	type Req struct {
		AccountID      string `json:"accountID"`
		MaxSpaceSize   int    `json:"maxSpaceSize"`
		MaxObjectCount int    `json:"maxObjectCount"`
		Enable         bool   `json:"enable"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	if pe.accountID != meta.GenerateAccountID(req.AccountID) {
		logger.GetLogger("dedups3").Errorf("invalid account id: %v", pe.accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "invalid account id", nil, http.StatusForbidden)
	}

	if pe.ac.Quota != nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusConflict, "quota already exists", nil, http.StatusConflict)
		return
	}

	err := pe.iam.SetQuota(pe.accountID, &meta.QuotaConfig{
		MaxSpaceSize:   req.MaxSpaceSize,
		MaxObjectCount: req.MaxObjectCount,
		Enable:         req.Enable,
	})

	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set quota config: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}

		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "create quota config failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminUpdateQuotaHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminUpdateQuotaHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}

	type Req struct {
		AccountID      string `json:"accountID"`
		MaxSpaceSize   int    `json:"maxSpaceSize"`
		MaxObjectCount int    `json:"maxObjectCount"`
		Enable         bool   `json:"enable"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	err := pe.iam.SetQuota(pe.accountID, &meta.QuotaConfig{
		MaxSpaceSize:   req.MaxSpaceSize,
		MaxObjectCount: req.MaxObjectCount,
		Enable:         req.Enable,
	})

	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to update quota config: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}

		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "update quota config failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminDeleteQuotaHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call AdminDeleteQuotaHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}

	err := pe.iam.DeleteQuota(pe.accountID)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete quota config: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}

		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchKey)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "quota not exists", nil, http.StatusNotFound)
			return
		}

		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "delete quota config failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminListChunkConfigHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminListChunkConfigHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}

	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("dedups3").Debugf("[Call adminListChunkConfigHandler] storage service is nil")
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "storage service is nil", nil, http.StatusInternalServerError)
		return
	}

	storages := ss.ListStorages()
	chunks := make(map[string]*meta.ChunkConfig)
	for _, _storage := range storages {
		if _storage.Chunk != nil {
			chunks[_storage.ID] = _storage.Chunk
		}
	}

	xhttp.AdminWriteJSONError(w, r, 0, "success", chunks, http.StatusOK)
}

func AdminGetChunkConfigHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminGetChunkConfigHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	query := utils.DecodeQuerys(r.URL.Query())
	storageID := query.Get("storageID")

	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("dedups3").Debugf("[Call adminListChunkConfigHandler] storage service is nil")
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "storage service is nil", nil, http.StatusInternalServerError)
		return
	}
	_storage, err := ss.GetStorage(storageID)
	if err != nil || _storage == nil {
		logger.GetLogger("dedups3").Errorf("failed to get storage %s: %v", storageID, err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to get storage", nil, http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"storageID": _storage.ID,
		"chunkSize": _storage.Chunk.ChunkSize,
		"fixSize":   _storage.Chunk.FixSize,
		"encrypt":   _storage.Chunk.Encrypt,
		"compress":  _storage.Chunk.Compress,
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", resp, http.StatusOK)
}

func AdminSetChunkConfigHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminSetChunkConfigHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	type Req struct {
		StorageID string `json:"storageID"`
		ChunkSize int32  `json:"chunkSize"`
		FixSize   bool   `json:"fixSize"`
		Encrypt   bool   `json:"encrypt"`
		Compress  bool   `json:"compress"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	ss := storage.GetStorageService()
	if ss == nil {
		logger.GetLogger("dedups3").Debugf("[Call adminListChunkConfigHandler] storage service is nil")
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "storage service is nil", nil, http.StatusInternalServerError)
		return
	}

	err := ss.SetChunkConfig(req.StorageID, &meta.ChunkConfig{
		ChunkSize: req.ChunkSize,
		FixSize:   req.FixSize,
		Encrypt:   req.Encrypt,
		Compress:  req.Compress,
	})

	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to update chunk config: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
			return
		}

		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "update chunk config failed", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminListStorageHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminListStorageHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	bs := storage.GetStorageService()
	if bs == nil {
		logger.GetLogger("dedups3").Errorf("storage service not initial")
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "storage service not initial", nil, http.StatusInternalServerError)
		return

	}
	storages := bs.ListStorages()
	type Resp struct {
		StorageID    string            `json:"storageID"`
		StorageClass string            `json:"storageClass"`
		StorageType  string            `json:"storageType"`
		S3           *xconf.S3Config   `json:"s3,omitempty"`
		Disk         *xconf.DiskConfig `json:"disk,omitempty"`
	}

	resp := make([]*Resp, 0, len(storages))
	for _, s := range storages {
		if s == nil {
			continue
		}
		_s := &Resp{
			StorageID:    s.ID,
			StorageClass: s.Class,
			StorageType:  s.Type,
		}
		if strings.ToLower(s.Type) == meta.S3_TYPE_STORAGE && s.Conf.S3 != nil {
			_s.S3 = s.Conf.S3
			resp = append(resp, _s)
		} else if strings.ToLower(s.Type) == meta.DISK_TYPE_STORAGE && s.Conf.Disk != nil {
			_s.Disk = s.Conf.Disk
			resp = append(resp, _s)
		}
	}
	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", &resp, http.StatusOK)
}

func AdminCreateStorageHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminCreateStorageHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	bs := storage.GetStorageService()
	if bs == nil {
		logger.GetLogger("dedups3").Errorf("storage service not initial")
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "storage service not initial", nil, http.StatusInternalServerError)
		return
	}

	type Req struct {
		StorageID    string            `json:"storageID"`
		StorageClass string            `json:"storageClass"`
		StorageType  string            `json:"storageType"`
		S3           *xconf.S3Config   `json:"s3,omitempty"`
		Disk         *xconf.DiskConfig `json:"disk,omitempty"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	req.StorageType = strings.ToLower(req.StorageType)
	req.StorageClass = strings.ToUpper(req.StorageClass)
	req.StorageID = strings.TrimSpace(req.StorageID)
	req.StorageType = strings.TrimSpace(req.StorageType)
	req.StorageClass = strings.TrimSpace(req.StorageClass)

	if !meta.IsValidIAMName(req.StorageID) ||
		(req.StorageClass != meta.STANDARD_CLASS_STORAGE && req.StorageClass != meta.STANDARD_IA_CLASS_STORAGE && req.StorageClass != meta.GLACIER_IR_CLASS_STORAGE) ||
		(req.StorageType != meta.DISK_TYPE_STORAGE && req.StorageType != meta.S3_TYPE_STORAGE) {
		logger.GetLogger("dedups3").Errorf("invalid request params %#v", req)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request params", nil, http.StatusBadRequest)
		return
	}

	var err error
	var _storage *meta.Storage

	switch strings.ToLower(req.StorageType) {
	case meta.S3_TYPE_STORAGE:
		if req.S3 == nil {
			logger.GetLogger("dedups3").Errorf("miss s3 config detail")
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "miss s3 config detail", nil, http.StatusBadRequest)
			return
		}
		_storage, err = bs.AddStorage(meta.S3_TYPE_STORAGE, strings.ToUpper(req.StorageClass), xconf.StorageConfig{
			ID:    req.StorageID,
			Class: req.StorageClass,
			S3:    req.S3,
		})
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to add s3 config: %v", err)
			xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to add s3 config", nil, http.StatusInternalServerError)
			return
		}
	case meta.DISK_TYPE_STORAGE:
		if req.Disk == nil {
			logger.GetLogger("dedups3").Errorf("miss disk config detail")
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "miss disk config detail", nil, http.StatusBadRequest)
			return
		}
		if !path.IsAbs(req.Disk.Path) {
			logger.GetLogger("dedups3").Errorf("invalid disk path: %s", req.Disk.Path)
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid disk path", nil, http.StatusBadRequest)
			return
		}
		req.Disk.Path += "/block"
		_storage, err = bs.AddStorage(meta.DISK_TYPE_STORAGE, strings.ToUpper(req.StorageClass), xconf.StorageConfig{
			ID:    req.StorageID,
			Class: req.StorageClass,
			Disk:  req.Disk,
		})
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to add disk config: %v", err)
			xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to add disk config", nil, http.StatusInternalServerError)
			return
		}
	}

	if _storage == nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to add _storage", nil, http.StatusInternalServerError)
		return
	}

	// 关键：检查 inst 是否也实现了 vfs.SyncTarget
	syncTargetor, ok := _storage.Instance.(vfs.SyncTargetor) // 类型断言
	if !ok {
		logger.GetLogger("dedups3").Errorf("storage instance for id %#v does not implement SyncTarget", _storage)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to add _storage", nil, http.StatusInternalServerError)
		return
	}

	vfile, err := block2.GetTieredFs()
	if err == nil && vfile != nil {
		_ = vfile.AddSyncTargetor(_storage.ID, syncTargetor)
	} else {
		logger.GetLogger("dedups3").Errorf("failed to get tiered vfs for storage %#v", _storage)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to add _storage", nil, http.StatusInternalServerError)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminTestStorageHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminTestStorageHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}

	type Req struct {
		StorageID    string            `json:"storageID"`
		StorageClass string            `json:"storageClass"`
		StorageType  string            `json:"storageType"`
		S3           *xconf.S3Config   `json:"s3,omitempty"`
		Disk         *xconf.DiskConfig `json:"disk,omitempty"`
	}

	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}
	req.StorageType = strings.ToLower(req.StorageType)
	req.StorageClass = strings.ToUpper(req.StorageClass)
	req.StorageID = strings.TrimSpace(req.StorageID)
	req.StorageType = strings.TrimSpace(req.StorageType)
	req.StorageClass = strings.TrimSpace(req.StorageClass)

	if !meta.IsValidIAMName(req.StorageID) ||
		(req.StorageClass != meta.STANDARD_CLASS_STORAGE && req.StorageClass != meta.STANDARD_IA_CLASS_STORAGE && req.StorageClass != meta.GLACIER_IR_CLASS_STORAGE) ||
		(req.StorageType != meta.DISK_TYPE_STORAGE && req.StorageType != meta.S3_TYPE_STORAGE) {
		logger.GetLogger("dedups3").Errorf("invalid request params %#v", req)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request params", nil, http.StatusBadRequest)
		return
	}

	switch strings.ToLower(req.StorageType) {
	case meta.S3_TYPE_STORAGE:
		if req.S3 == nil {
			logger.GetLogger("dedups3").Errorf("miss s3 config detail")
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "miss s3 config detail", nil, http.StatusBadRequest)
			return
		}
		err := block2.TestS3AccessPermissions(req.S3)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("s3 storage test failed: %v", err)
			xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "s3 storage test failed", map[string]interface{}{"error": err.Error()}, http.StatusInternalServerError)
			return
		}

	case meta.DISK_TYPE_STORAGE:
		if req.Disk == nil {
			logger.GetLogger("dedups3").Errorf("miss disk config detail")
			xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "miss disk config detail", nil, http.StatusBadRequest)
			return
		}
		err := block2.TestDiskAccessPermissions(req.Disk)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("disk storage test failed: %v", err)
			xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "disk storage test failed", map[string]interface{}{"error": err.Error()}, http.StatusInternalServerError)
			return
		}
	default:
		logger.GetLogger("dedups3").Errorf("unsupported storage type: %s", req.StorageType)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "unsupported storage type", nil, http.StatusBadRequest)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "storage test passed", nil, http.StatusOK)
}

func AdminDeleteStorageHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Debugf("[Call adminDeleteStorageHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}

	query := utils.DecodeQuerys(r.URL.Query())
	storageID := query.Get("storageID")

	bs := storage.GetStorageService()
	if bs == nil {
		logger.GetLogger("dedups3").Errorf("storage service not initial")
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "storage service not initial", nil, http.StatusInternalServerError)
		return
	}

	bs.RemoveStorage(storageID)

	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminDebugObjectInfoHandler(w http.ResponseWriter, r *http.Request) {
	query := utils.DecodeQuerys(r.URL.Query())
	objectID := query.Get("objectID")
	objectID = strings.TrimSpace(objectID)
	kvstore, err := kv.GetKvStore()
	if err != nil || kvstore == nil {
		logger.GetLogger("dedups3").Errorf("failed to get kvstore: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to get kvstore", nil, http.StatusInternalServerError)
		return
	}

	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}

	objkey := "aws:object:" + pe.accountID + ":" + objectID

	var _object meta.Object
	exist, err := kvstore.Get(objkey, &_object)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to fetch object %s: %v", objkey, err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to fetch object", nil, http.StatusInternalServerError)
		return
	}
	if !exist {
		logger.GetLogger("dedups3").Infof("object %s does not exist", objkey)
		xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "object not found", nil, http.StatusNotFound)
		return
	}
	xhttp.AdminWriteJSONError(w, r, 0, "success", &_object.BaseObject, http.StatusOK)
}

func AdminDebugBlockInfoHandler(w http.ResponseWriter, r *http.Request) {
	query := utils.DecodeQuerys(r.URL.Query())
	blockID := query.Get("blockID")
	blockID = strings.TrimSpace(blockID)
	kvstore, err := kv.GetKvStore()
	if err != nil || kvstore == nil {
		logger.GetLogger("dedups3").Errorf("failed to get kvstore: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to get kvstore", nil, http.StatusInternalServerError)
		return
	}

	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}

	blockkey := "aws:block:" + blockID

	var _block meta.Block
	exist, err := kvstore.Get(blockkey, &_block)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to fetch block %s: %v", blockkey, err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to fetch block", nil, http.StatusInternalServerError)
		return
	}
	if !exist {
		logger.GetLogger("dedups3").Infof("block %s does not exist", blockkey)
		xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "block not found", nil, http.StatusNotFound)
		return
	}
	xhttp.AdminWriteJSONError(w, r, 0, "success", &_block.BlockHeader, http.StatusOK)
}

func AdminDebugChunkInfoHandler(w http.ResponseWriter, r *http.Request) {
	query := utils.DecodeQuerys(r.URL.Query())
	chunkID := query.Get("chunkID")
	chunkID = strings.TrimSpace(chunkID)
	kvstore, err := kv.GetKvStore()
	if err != nil || kvstore == nil {
		logger.GetLogger("dedups3").Errorf("failed to get kvstore: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to get kvstore", nil, http.StatusInternalServerError)
		return
	}

	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}

	chunkkey := "aws:chunk:" + chunkID

	var _chunk meta.Chunk
	exist, err := kvstore.Get(chunkkey, &_chunk)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to fetch chunk %s: %v", chunkkey, err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to fetch block", nil, http.StatusInternalServerError)
		return
	}
	if !exist {
		logger.GetLogger("dedups3").Infof("chunk %s does not exist", chunkkey)
		xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "chunk not found", nil, http.StatusNotFound)
		return
	}
	xhttp.AdminWriteJSONError(w, r, 0, "success", &_chunk, http.StatusOK)
}
