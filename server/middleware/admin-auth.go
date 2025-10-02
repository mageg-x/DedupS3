package middleware

import (
	"context"
	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/utils"
	"net/http"
)

func AdminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 允许跨域并支持凭据
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		if r.Method == "OPTIONS" {
			// 预检请求：加 Methods 和 Headers
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusOK)
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
		username, newToken, err := utils.VerifyToken(tokenString)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Error verifying token: %v", err)
			xhttp.AdminWriteJSONError(w, r, http.StatusUnauthorized, "invalid token", nil, http.StatusUnauthorized)
			return
		}

		// 将用户信息注入请求上下文（供后续 handler 使用）
		if username != "" {
			ctx := context.WithValue(r.Context(), "username", username)
			r = r.WithContext(ctx)
		}

		// 如果有新 token，通过响应头返回 ===
		if newToken != "" {
			// 设置 Cookie
			http.SetCookie(w, &http.Cookie{
				Name:     "access_token",          // Cookie 名字
				Value:    newToken,                // Token 值
				Path:     "/",                     // 作用路径
				HttpOnly: true,                    // 关键：JS 无法读取，防 XSS
				Secure:   true,                    // 仅 HTTPS（开发环境可设 false）
				SameSite: http.SameSiteStrictMode, // 防 CSRF
				MaxAge:   3600,                    // 有效期 1 小时（秒）
			})
		}

		next.ServeHTTP(w, r)
	})
}
