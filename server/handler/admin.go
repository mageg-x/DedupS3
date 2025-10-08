package handler

import (
	"archive/zip"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mageg-x/boulder/middleware"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	xconf "github.com/mageg-x/boulder/internal/config"
	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
	sb "github.com/mageg-x/boulder/service/bucket"
	iam2 "github.com/mageg-x/boulder/service/iam"
	"github.com/mageg-x/boulder/service/object"
	"github.com/mageg-x/boulder/service/stats"
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
		logger.GetLogger("boulder").Errorf("failed to get iam service")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "service unavailable", nil, http.StatusServiceUnavailable)
		return nil
	}

	ac, err := iamService.GetAccount(accountID)
	if err != nil || ac == nil {
		logger.GetLogger("boulder").Errorf("failed to get account %s", accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "account not found", nil, http.StatusForbidden)
		return nil
	}

	user, err := ac.GetUser(username)
	if err != nil || user == nil || len(user.AccessKeys) == 0 {
		logger.GetLogger("boulder").Errorf("failed to get root user for account %s", accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "service unavailable", nil, http.StatusServiceUnavailable)
		return nil
	}

	accessKeyID := user.AccessKeys[0].AccessKeyID
	logger.GetLogger("boulder").Errorf("account %s  user %s has access key %s", accountID, username, accessKeyID)
	return &PrepareEnv{
		username:  username,
		accountID: accountID,
		accessKey: accessKeyID,
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
		logger.GetLogger("boulder").Errorf("bucket service not initialized")
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

		logger.GetLogger("boulder").Errorf("get  bucket %s info failed: %v", bucketName, err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "failed to upload file", nil, http.StatusInternalServerError)
		return nil
	}

	// 获取对象服务
	_os := object.GetObjectService()
	if _os == nil {
		logger.GetLogger("boulder").Errorf("object service not initialized")
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
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.GetLogger("boulder").Errorf("failed decoding request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid params", nil, http.StatusBadRequest)
		return
	}

	loginname := strings.TrimSpace(req.Username)
	username, account := middleware.ParseLoginUsername(loginname)
	if err := meta.ValidateUsername(username); err != nil {
		logger.GetLogger("boulder").Errorf("username %s is invalid format", username)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid username", nil, http.StatusBadRequest)
		return
	}
	accountID := meta.GenerateAccountID(account)

	iam := iam2.GetIamService()
	ac, err := iam.GetAccount(accountID)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get account %s", req.Username)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "failed to get account", nil, http.StatusBadRequest)
		return
	}
	// 验证用户名密码
	user := ac.Users[username]
	if user == nil {
		logger.GetLogger("boulder").Errorf("user %s not found", username)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "user not found", nil, http.StatusBadRequest)
		return
	}

	password := strings.TrimSpace(req.Password)
	inputStr := user.Password + ":" + loginname
	outputStr := md5.Sum([]byte(inputStr))
	expectStr := hex.EncodeToString(outputStr[:])
	if expectStr != password {
		logger.GetLogger("boulder").Errorf("password mismatch for user %s, %s:%s", username, password, expectStr)
		xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "passwords mismatch", nil, http.StatusUnauthorized)
		return
	}

	// 生成 JWT
	token, err := utils.GenerateToken(loginname)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to generate token for user %s", username)
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
	logger.GetLogger("boulder").Errorf("[Call GetStatsHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	ss := stats.GetStatsService()
	_stats, err := ss.GetAccountStats(pe.accountID)
	if err != nil || _stats == nil {
		logger.GetLogger("boulder").Errorf("failed to get stats for account %s", pe.username)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "internal server error", nil, http.StatusInternalServerError)
		return
	}

	xhttp.AdminWriteJSONError(w, r, 0, "success", _stats, http.StatusOK)
}

func AdminListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[Call ListBucketHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}
	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}

	buckets, _, err := bs.ListBuckets(&sb.BaseBucketParams{
		AccessKeyID: pe.accessKey,
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to list buckets: %v", err)
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
				logger.GetLogger("boulder").Errorf("get bucket %s stats not found", _bucket.Name)
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
	logger.GetLogger("boulder").Errorf("[call createbuckethandler] %#v", r.URL)
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
		logger.GetLogger("boulder").Errorf("bucket service is nil")
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
		logger.GetLogger("boulder").Errorf("failed create bucket err: %v", err)

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
	logger.GetLogger("boulder").Errorf("[call deletebuckethandler] %#v", r.URL)
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
		logger.GetLogger("boulder").Errorf("failed create bucket err: %v", err)

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
	logger.GetLogger("boulder").Errorf("[call adminlistobjectshandler] %#v", r.URL)

	query := utils.DecodeQuerys(r.URL.Query())
	bucketName := query.Get("bucket")
	bucketName = strings.TrimSpace(bucketName)
	prefix := query.Get("prefix")
	prefix = strings.TrimSpace(prefix)
	// 将多个连续的 / 替换成一个 /
	prefix = regexp.MustCompile("/+").ReplaceAllString(prefix, "/")

	marker := query.Get("marker")
	delimiter := "/"
	logger.GetLogger("boulder").Debugf("get query %#v  prefix %s", query, prefix)

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

		logger.GetLogger("boulder").Errorf("error listing objects: %s", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "internal server error", nil, http.StatusInternalServerError)
		return
	}

	logger.GetLogger("boulder").Infof("bucket %s listobjects: files %d, folders %d  nextmarker %s has more %v",
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
	logger.GetLogger("boulder").Errorf("[call admincreatefolderhandler] %#v", r.URL)

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
		logger.GetLogger("boulder").Errorf("invalid bucket or folder")
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

		logger.GetLogger("boulder").Errorf("error create folder objects: %s", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "internal server error", nil, http.StatusInternalServerError)
		return
	}

	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func AdminPutObjectHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[call adminputobjecthandler]")
	// 限制请求体大小
	const maxRequestSize = 500 << 20 // 500MB
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)
	// 设置一个较小的内存限制，大文件会自动写入临时文件
	const maxMemory = 10 << 20 // 10MB
	err := r.ParseMultipartForm(maxMemory)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to parse multipart form: %v", err)
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

	logger.GetLogger("boulder").Errorf(" bucket: %s, object: %s, contenttype: %s", bucketName, objectName, contentType)
	if err := utils.CheckValidObjectName(objectName); err != nil || bucketName == "" || objectName == "" {
		logger.GetLogger("boulder").Errorf("invalid bucket or object name")
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
		logger.GetLogger("boulder").Errorf("failed to get file from form: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "missing file", nil, http.StatusBadRequest)
		return
	}
	defer file.Close()

	if contentType == "" {
		contentType = fileHeader.Header.Get("Content-Type")
	}

	// 记录文件信息
	logger.GetLogger("boulder").Infof("uploading file: %s, size: %d bytes", fileHeader.Filename, fileHeader.Size)

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
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "AccessDenied", nil, http.StatusForbidden)
			return
		}
		if errors.Is(err, xhttp.ToError(xhttp.ErrNoSuchBucket)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "NoSuchBucket", nil, http.StatusNotFound)
			return
		}

		logger.GetLogger("boulder").Errorf("error uploading object: %v", err)
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
	logger.GetLogger("boulder").Errorf("[call admindelobjecthandler]")

	type Req struct {
		BucketName string   `json:"bucket"`
		Keys       []string `json:"keys"` // 对象键或目录路径列表
	}
	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.GetLogger("boulder").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}
	logger.GetLogger("boulder").Errorf("get delete request object list : %#v", req.Keys)

	// 验证参数
	req.BucketName = strings.TrimSpace(req.BucketName)
	if req.BucketName == "" || len(req.Keys) == 0 {
		logger.GetLogger("boulder").Errorf("invalid bucket name or empty keys list")
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
					logger.GetLogger("boulder").Errorf("error listing objects of folder %s : %v", key, err)
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
	logger.GetLogger("boulder").Errorf("Prepare4S3 to delete key %#v", allObjectKeys)
	deleteKeys := make([]string, 0, len(allObjectKeys))
	for _, objkey := range allObjectKeys {
		// 执行删除操作
		err := pe.os.DeleteObject(&object.BaseObjectParams{
			BucketName:  req.BucketName,
			ObjKey:      objkey,
			AccessKeyID: pe.accessKey,
		})
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to delete object %s: %v", objkey, err)
		} else {
			deleteKeys = append(deleteKeys, objkey)
		}
	}
	// 返回成功响应
	resp := map[string]interface{}{
		"bucket":  req.BucketName,
		"deleted": len(deleteKeys),
	}

	logger.GetLogger("boulder").Infof("successfully deleted %d objects from bucket %s", len(deleteKeys), req.BucketName)
	xhttp.AdminWriteJSONError(w, r, 0, "success", resp, http.StatusOK)
}

func AdminGetObjectHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[call admingetobjecthandler] %#v", r.URL)
	type Req struct {
		BucketName string   `json:"bucket"`
		Files      []string `json:"files"`
		Filename   string   `json:"filename"`
	}
	// 解析请求体
	var req Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Files) == 0 {
		logger.GetLogger("boulder").Errorf("failed to decode request: %v", err)
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
	logger.GetLogger("boulder").Errorf("download object list : %#v", objectKeys)

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
					logger.GetLogger("boulder").Errorf("error listing objects of folder %s : %v", key, err)
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
	logger.GetLogger("boulder").Errorf("Prepare4S3 to download key %#v", allObjectKeys)
	if len(allObjectKeys) == 0 {
		logger.GetLogger("boulder").Errorf("no object to download")
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

			logger.GetLogger("boulder").Errorf("failed to get object %s: %v", allObjectKeys[0], err)
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
				logger.GetLogger("boulder").Infof("client disconnected during download: %v", err)
			} else {
				logger.GetLogger("boulder").Errorf("write response body failed: %v", err)
			}
			return
		}

		logger.GetLogger("boulder").Errorf("successfully downloaded object %s from bucket %s", allObjectKeys[0], req.BucketName)
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
					logger.GetLogger("boulder").Errorf("copy object %s to zip failed: %v", key, err)
				}
			} else {
				logger.GetLogger("boulder").Errorf("failed to get object %s: %v", key, err)
			}
		}

		// 确保zip写入器被刷新
		if err := zipWriter.Flush(); err != nil {
			logger.GetLogger("boulder").Errorf("failed to flush zip writer: %v", err)
		}

		logger.GetLogger("boulder").Infof("successfully downloaded %d objects as zip from bucket %s", len(allObjectKeys), req.BucketName)
	}
}

func AdminListUserHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[call adminListUserHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	userList := make([]*IamUserInfo, 0)

	for _, u := range pe.ac.Users {
		if u.IsRoot {
			continue
		}
		ui := &IamUserInfo{
			Username:       u.Username,
			Account:        pe.ac.Name,
			Group:          utils.StringSlice(u.Groups),
			Role:           utils.StringSlice(u.Roles),
			AttachPolicies: utils.StringSlice(u.AttachedPolicies),
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
	logger.GetLogger("boulder").Errorf("[call adminGetUserHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}

	userInfo := &IamUserInfo{
		Username:       pe.username,
		Account:        pe.ac.Name,
		Group:          utils.StringSlice(pe.user.Groups),
		Role:           utils.StringSlice(pe.user.Roles),
		AttachPolicies: utils.StringSlice(pe.user.AttachedPolicies),
		AllPolicies:    make([]meta.Statement, 0),
		Enabled:        pe.user.Enabled,
		CreatedAt:      pe.user.CreatedAt,
	}

	allPolices := pe.ac.GetUserAllPolicies(pe.user)
	for _, policy := range allPolices {
		if policy != nil && policy.Document != "" {
			var pd meta.PolicyDocument
			err := json.Unmarshal([]byte(policy.Document), &pd)
			if err != nil {
				logger.GetLogger("boulder").Errorf("failed to unmarshal policy %s: %v", policy.Name, err)
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

func AdminSetUserHandler(w http.ResponseWriter, r *http.Request) {

}

func AdminCreateUserHandler(w http.ResponseWriter, r *http.Request) {

}

func AdminDeleteUserHandler(w http.ResponseWriter, r *http.Request) {

}

func AdminListPolicyHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[call adminListPolicyHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}
	policies := make([]*meta.IamPolicy, 0)
	if pe.ac.Policies != nil {
		for _, policy := range pe.ac.Policies {
			policies = append(policies, policy)
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

	if p, exists := pe.ac.Policies[name]; !exists || p == nil {
		logger.GetLogger("boulder").Errorf("policy %s does not exist", name)
		xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "policy name does not exist", nil, http.StatusNotFound)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", pe.ac.Policies[name], http.StatusOK)
}

func AdminSetPolicyHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[call adminListPolicyHandler] %#v", r.URL)
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
		logger.GetLogger("boulder").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}
	if pe.ac == nil || pe.user == nil {
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "invalid policy document", nil, http.StatusInternalServerError)
		return
	}

	err := pe.iam.UpdatePolicy(pe.accountID, pe.username, req.Name, req.Desc, req.Doc)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to update policy: %v", err)
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
	logger.GetLogger("boulder").Errorf("[call adminListPolicyHandler] %#v", r.URL)
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
		logger.GetLogger("boulder").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}
	err := pe.iam.CreatePolicy(pe.accountID, pe.username, req.Name, req.Desc, req.Doc)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to create policy: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
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
		logger.GetLogger("boulder").Errorf("failed to delete policy: %v", err)
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

func AdminListGroupHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[call adminListGroupHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil {
		return
	}
	groups := make([]*meta.IamGroup, 0)
	if pe.ac.Groups != nil {
		for _, group := range pe.ac.Groups {
			groups = append(groups, group)
		}
	}
	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", groups, http.StatusOK)
}

func AdminListRoleHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[call adminListRoleHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	roles := make([]*meta.IamRole, 0)
	if pe.ac.Policies != nil {
		for _, role := range pe.ac.Roles {
			roles = append(roles, role)
		}
	}
	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", roles, http.StatusOK)
}

func AdminGetRoleHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[call adminGetRoleHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	query := utils.DecodeQuerys(r.URL.Query())
	name := query.Get("name")
	name = strings.TrimSpace(name)
	if p, exists := pe.ac.Roles[name]; !exists || p == nil {
		logger.GetLogger("boulder").Errorf("role %s does not exist", name)
		xhttp.AdminWriteJSONError(w, r, http.StatusNotFound, "role name does not exist", nil, http.StatusNotFound)
		return
	}

	// 返回成功响应
	xhttp.AdminWriteJSONError(w, r, 0, "success", pe.ac.Roles[name], http.StatusOK)
}

func AdminCreateRoleHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[call adminCreateRoleHandler] %#v", r.URL)
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
		logger.GetLogger("boulder").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	err := pe.iam.CreateRole(pe.accountID, pe.username, req.Name, req.Desc, "", req.AttachPolicies)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to create role: %v", err)
		if errors.Is(err, xhttp.ToError(xhttp.ErrAccessDenied)) {
			xhttp.AdminWriteJSONError(w, r, http.StatusForbidden, "access denied", nil, http.StatusForbidden)
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
	logger.GetLogger("boulder").Errorf("[call adminUpdateRoleHandler] %#v", r.URL)
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
		logger.GetLogger("boulder").Errorf("failed to decode request: %v", err)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid request body", nil, http.StatusBadRequest)
		return
	}

	err := pe.iam.UpdateRole(pe.accountID, pe.username, req.Name, req.Desc, "", req.AttachPolicies)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to update role: %v", err)
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
	logger.GetLogger("boulder").Errorf("[call adminDeleteRoleHandler] %#v", r.URL)
	pe := Prepare4Iam(w, r)
	if pe == nil || pe.ac == nil {
		return
	}
	query := utils.DecodeQuerys(r.URL.Query())
	name := query.Get("name")
	name = strings.TrimSpace(name)

	if err := pe.iam.DeleteRole(pe.accountID, pe.username, name); err != nil {
		logger.GetLogger("boulder").Errorf("failed to delete role: %v", err)
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
