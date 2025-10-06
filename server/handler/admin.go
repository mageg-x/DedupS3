package handler

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	xconf "github.com/mageg-x/boulder/internal/config"
	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
	sb "github.com/mageg-x/boulder/service/bucket"
	iam2 "github.com/mageg-x/boulder/service/iam"
	"github.com/mageg-x/boulder/service/stats"
)

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

	username := strings.TrimSpace(req.Username)
	if err := meta.ValidateUsername(username); err != nil {
		logger.GetLogger("boulder").Errorf("username %s is invalid format", username)
		xhttp.AdminWriteJSONError(w, r, http.StatusBadRequest, "invalid username", nil, http.StatusBadRequest)
		return
	}
	accountID := meta.GenerateAccountID(req.Username)

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
	inputStr := user.Password + ":" + user.Username
	outputStr := md5.Sum([]byte(inputStr))
	expectStr := hex.EncodeToString(outputStr[:])
	if expectStr != password {
		logger.GetLogger("boulder").Errorf("password mismatch for user %s, %s:%s", username, password, expectStr)
		xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "passwords mismatch", nil, http.StatusUnauthorized)
		return
	}

	// 生成 JWT
	token, err := utils.GenerateToken(username)
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
	username, _ := r.Context().Value("username").(string)
	if username == "" {
		xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "invalid username", nil, http.StatusUnauthorized)
		return
	}

	accountID := meta.GenerateAccountID(username)
	ss := stats.GetStatsService()
	_stats, err := ss.GetAccountStats(accountID)
	if err != nil || _stats == nil {
		logger.GetLogger("boulder").Errorf("failed to get stats for account %s", username)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "internal server error", nil, http.StatusInternalServerError)
		return
	}

	xhttp.AdminWriteJSONError(w, r, 0, "success", _stats, http.StatusOK)
}

func AdminListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[Call ListBucketHandler] %#v", r.URL)
	username, _ := r.Context().Value("username").(string)
	if username == "" {
		xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "invalid username", nil, http.StatusUnauthorized)
		return
	}
	accountID := meta.GenerateAccountID(username)

	iamService := iam2.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("Failed to get IAM service")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "invalid username", nil, http.StatusServiceUnavailable)
		return
	}

	ac, err := iamService.GetAccount(accountID)
	if err != nil || ac == nil {
		logger.GetLogger("boulder").Errorf("failed to get account %s", accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}

	adminUser, err := ac.GetUser("root")
	if err != nil || adminUser == nil || len(adminUser.AccessKeys) == 0 {
		logger.GetLogger("boulder").Errorf("failed to get root user for account %s", accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}

	accessKeyID := adminUser.AccessKeys[0].AccessKeyID

	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}

	buckets, _, err := bs.ListBuckets(&sb.BaseBucketParams{
		AccessKeyID: accessKeyID,
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
	for _, _bucket := range buckets {
		if _bucket != nil {
			bucket := Bucket{
				Base: _bucket,
			}
			if _stats, err := ss.GetBucketStats(_bucket.Name); err == nil && _stats != nil {
				bucket.Stats = _stats
			}
			bucketList = append(bucketList, bucket)
		}
	}

	xhttp.AdminWriteJSONError(w, r, 0, "success", bucketList, http.StatusOK)
}

func AdminCreateBucketHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[Call CreateBucketHandler] %#v", r.URL)
	username, _ := r.Context().Value("username").(string)
	if username == "" {
		xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "invalid username", nil, http.StatusUnauthorized)
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

	accountID := meta.GenerateAccountID(username)

	iamService := iam2.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("Failed to get IAM service")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "invalid username", nil, http.StatusServiceUnavailable)
		return
	}

	ac, err := iamService.GetAccount(accountID)
	if err != nil || ac == nil {
		logger.GetLogger("boulder").Errorf("failed to get account %s", accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}

	adminUser, err := ac.GetUser("root")
	if err != nil || adminUser == nil || len(adminUser.AccessKeys) == 0 {
		logger.GetLogger("boulder").Errorf("failed to get root user for account %s", accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}

	accessKeyID := adminUser.AccessKeys[0].AccessKeyID

	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}
	cfg := xconf.Get()
	err = bs.CreateBucket(&sb.BaseBucketParams{
		BucketName:        req.Name,
		Location:          cfg.Node.Region,
		ObjectLockEnabled: false,
		AccessKeyID:       accessKeyID,
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
	logger.GetLogger("boulder").Errorf("[Call DeleteBucketHandler] %#v", r.URL)
	username, _ := r.Context().Value("username").(string)
	if username == "" {
		xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "invalid username", nil, http.StatusUnauthorized)
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

	accountID := meta.GenerateAccountID(username)

	iamService := iam2.GetIamService()
	if iamService == nil {
		logger.GetLogger("boulder").Errorf("Failed to get IAM service")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "invalid username", nil, http.StatusServiceUnavailable)
		return
	}

	ac, err := iamService.GetAccount(accountID)
	if err != nil || ac == nil {
		logger.GetLogger("boulder").Errorf("failed to get account %s", accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}

	adminUser, err := ac.GetUser("root")
	if err != nil || adminUser == nil || len(adminUser.AccessKeys) == 0 {
		logger.GetLogger("boulder").Errorf("failed to get root user for account %s", accountID)
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}

	accessKeyID := adminUser.AccessKeys[0].AccessKeyID

	bs := sb.GetBucketService()
	if bs == nil {
		logger.GetLogger("boulder").Errorf("bucket service is nil")
		xhttp.AdminWriteJSONError(w, r, http.StatusServiceUnavailable, "internal server error", nil, http.StatusServiceUnavailable)
		return
	}

	err = bs.DeleteBucket(&sb.BaseBucketParams{
		BucketName:  req.Name,
		AccessKeyID: accessKeyID,
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
