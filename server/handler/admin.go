package handler

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"github.com/mageg-x/boulder/internal/config"
	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/service/stats"
	"net/http"
	"strings"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
	iam2 "github.com/mageg-x/boulder/service/iam"
)

// POST /api/login
func LoginHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	json.NewDecoder(r.Body).Decode(&req)

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
	if config.IsDev {
		sameSite = http.SameSiteLaxMode
	}
	// 设置 Cookie
	cookie := &http.Cookie{
		Name:     "access_token", // Cookie 名字
		Value:    token,          // Token 值
		Path:     "/",            // 作用路径
		HttpOnly: true,           // 关键：JS 无法读取，防 XSS
		Secure:   !config.IsDev,  // 仅 HTTPS（开发环境可设 false）
		SameSite: sameSite,       // 防 CSRF
		MaxAge:   3600,           // 有效期 1 小时（秒）
		// Expires: time.Now().Add(1 * time.Hour), // 过期时间（旧方式）
	}
	http.SetCookie(w, cookie)

	w.Header().Set("Content-Type", "application/json")

	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func LogoutHandler(w http.ResponseWriter, r *http.Request) {
	sameSite := http.SameSiteStrictMode
	if config.IsDev {
		sameSite = http.SameSiteLaxMode
	}

	// 清除 Cookie：设置 Max-Age=-1 或空值
	http.SetCookie(w, &http.Cookie{
		Name:     "access_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   !config.IsDev, // 生产环境必须为 true
		SameSite: sameSite,
		MaxAge:   -1, // 表示立即过期
	})

	w.Header().Set("Content-Type", "application/json")
	xhttp.AdminWriteJSONError(w, r, 0, "success", nil, http.StatusOK)
}

func GetStatsHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("boulder").Errorf("[Call GetStatsHandler] %#v", r.URL)
	username, _ := r.Context().Value("username").(string)
	if username == "" {
		xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "invalid username", nil, http.StatusUnauthorized)
		return
	}

	accountID := meta.GenerateAccountID(username)
	ss := stats.GetStatsService()
	_stats, err := ss.GetStats(accountID)
	if err != nil || _stats == nil {
		logger.GetLogger("boulder").Errorf("failed to get stats for account %s", username)
		xhttp.AdminWriteJSONError(w, r, http.StatusInternalServerError, "internal server error", nil, http.StatusInternalServerError)
		return
	}

	xhttp.AdminWriteJSONError(w, r, 0, "success", _stats, http.StatusOK)
}
