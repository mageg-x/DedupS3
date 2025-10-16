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
package utils

import (
	"fmt"
	"github.com/mageg-x/dedups3/internal/logger"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	jwtSecret = []byte("NaBdfEhUZ5djyQQZZxUX3zhtM3hCWnru3FFaWHCp8jhUmyryWwC33Vph2NecEYWH") // 至少 32 字符
)

// 自定义声明（Claims）
type CustomClaims struct {
	Username string `json:"username"`
	Salt     string `json:"salt"`
	jwt.RegisteredClaims
}

// 生成 JWT Token
func GenerateToken(username string) (string, error) {
	// 设置过期时间
	expireTime := time.Now().Add(7 * 24 * time.Hour)

	// 构建自定义声明
	claims := &CustomClaims{
		Username: username,
		Salt:     RandString(16),
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expireTime), // 过期时间
			IssuedAt:  jwt.NewNumericDate(time.Now()), // 签发时间
			NotBefore: jwt.NewNumericDate(time.Now()), // 生效时间
		},
	}

	// 创建 token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// 签名并返回字符串
	tokenString, err := token.SignedString(jwtSecret)
	if err != nil {
		return "", err
	}

	return tokenString, nil
}

func VerifyToken(tokenString string) (string, string, error) {
	// 解析 token
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		// 确保签名算法正确
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return jwtSecret, nil
	})

	if err != nil {
		logger.GetLogger("dedups3").Errorf("invalid token %v", err)
		// 常见错误：过期、签名错误等
		return "", "", fmt.Errorf("invalid token %w", err)
	}

	// 检查 token 是否有效
	claims, ok := token.Claims.(*CustomClaims)
	if !ok || !token.Valid {
		logger.GetLogger("dedups3").Errorf("invalid token %v", err)
		// 常见错误：过期、签名错误等
		return "", "", fmt.Errorf("invalid token %w", err)
	}

	_expTime := claims.ExpiresAt.Time
	if time.Until(_expTime) < 24*time.Hour {
		newToken, err := GenerateToken(claims.Username)
		if err != nil {
			// 刷新失败不影响当前 token
			return claims.Username, "", nil
		}
		return claims.Username, newToken, nil
	}

	return claims.Username, "", nil
}
