package middleware

import (
	"context"
	"github.com/mageg-x/dedups3/internal/config"
	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/utils"
	"net/http"
	"strings"
)

func AdminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.GetLogger("dedups3").Debugf("access request %s %s", r.Method, r.URL.Path)

		// 允许跨域并支持凭据
		origin := r.Header.Get("Origin")
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		if r.Method == "OPTIONS" {
			// 预检请求：加 Methods 和 Headers
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// 白名单：跳过登录等公开接口
		if r.URL.Path == "/api/login" {
			next.ServeHTTP(w, r)
			return
		}

		// 从 Cookie 获取 token（替代 Authorization Header）
		cookie, err := r.Cookie("access_token")
		if err != nil {
			// Cookie 不存在或无效
			xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "missing or invalid access_token cookie", nil, http.StatusUnauthorized)
			return
		}
		tokenString := cookie.Value
		if tokenString == "" {
			xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "token is empty", nil, http.StatusUnauthorized)
			return
		}

		// 调用 VerifyToken 校验并可能返回新 token ===
		loginname, newToken, err := utils.VerifyToken(tokenString)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("Error verifying token: %v", err)
			xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "invalid token", nil, http.StatusUnauthorized)
			return
		}

		// 将用户信息注入请求上下文（供后续 handler 使用）
		loginname = strings.TrimSpace(loginname)

		//account 登录 直接account
		//user 登录 为 username@account
		//提取account 和 username
		username, account := ParseLoginUsername(loginname)
		if account == "" || username == "" {
			logger.GetLogger("dedups3").Errorf("failed get account name %s", loginname)
			xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "invalid token", nil, http.StatusUnauthorized)
		}
		ctx := r.Context()
		ctx = context.WithValue(ctx, "username", username)
		ctx = context.WithValue(ctx, "account", account)
		r = r.WithContext(ctx)

		// 如果有新 token，通过响应头返回 ===
		if newToken != "" {
			sameSite := http.SameSiteStrictMode
			if config.IsDev {
				sameSite = http.SameSiteLaxMode
			}
			// 设置 Cookie
			c := &http.Cookie{
				Name:     "access_token", // Cookie 名字
				Value:    newToken,       // Token 值
				Path:     "/",            // 作用路径
				HttpOnly: true,           // 关键：JS 无法读取，防 XSS
				Secure:   !config.IsDev,  // 仅 HTTPS（开发环境可设 false）
				SameSite: sameSite,       // 防 CSRF
				MaxAge:   3600,           // 有效期 1 小时（秒）
			}
			http.SetCookie(w, c)
		}

		next.ServeHTTP(w, r)
	})
}

// ParseLoginUsername 解析登录输入，返回 username 和 account
func ParseLoginUsername(input string) (username, account string) {
	if input == "" {
		return "", ""
	}

	if strings.Contains(input, "@") {
		parts := strings.SplitN(input, "@", 2) // 分成两部分，避免多个@出问题
		username = parts[0]
		account = parts[1]
	} else {
		// 只有 account，username 留空 或 设为 "admin" / account，视业务而定
		username = input // 或者 username = input （如果是主账号用户）
		account = input
	}
	return username, account
}
