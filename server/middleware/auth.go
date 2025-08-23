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

package middleware

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/service/iam"
)

// 错误定义
var (
	spaceRegex = regexp.MustCompile(`\s+`)
)

// AWS4SigningMiddleware 提供AWS4签名验证的中间件
func AWS4SigningMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 确保Host头存在
		if r.Header.Get("Host") == "" {
			r.Header.Set("Host", r.Host)
		}
		// 1. 从请求头中提取签名信息
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			xhttp.WriteAWSErr(w, r, xhttp.ErrMissingAuthenticationToken)
			return
		}

		// 解析Authorization头
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "AWS4-HMAC-SHA256" {
			xhttp.WriteAWSErr(w, r, xhttp.ErrAuthentication)
			return
		}

		// 解析凭证部分
		var credential, signedHeadersStr, signature string
		for _, part := range strings.Split(parts[1], ", ") {
			if strings.HasPrefix(part, "Credential=") {
				credential = strings.TrimPrefix(part, "Credential=")
			} else if strings.HasPrefix(part, "SignedHeaders=") {
				signedHeadersStr = strings.TrimPrefix(part, "SignedHeaders=")
			} else if strings.HasPrefix(part, "Signature=") {
				signature = strings.TrimPrefix(part, "Signature=")
			}
		}

		if credential == "" || signedHeadersStr == "" || signature == "" {
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}

		// 提取访问密钥ID
		credentialParts := strings.Split(credential, "/")
		if len(credentialParts) < 5 {
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}

		accessKeyID := credentialParts[0]
		date := credentialParts[1]
		region := credentialParts[2]
		service := credentialParts[3]

		// 3. 检查时间有效性
		amzDate := r.Header.Get("X-Amz-Date")
		if amzDate == "" {
			xhttp.WriteAWSErr(w, r, xhttp.ErrMissingDateHeader)
			return
		}

		// 验证时间格式
		if len(amzDate) != 16 {
			xhttp.WriteAWSErr(w, r, xhttp.ErrMalformedDate)
			return
		}

		parsedTime, err := time.Parse("20060102T150405Z", amzDate)
		if err != nil {
			xhttp.WriteAWSError(w, r, "MalformedDate", "Invalid X-Amz-Date format", http.StatusBadRequest)
			return
		}

		// 时间窗口：15分钟
		timeWindow := 15 * time.Minute
		now := time.Now().UTC()
		if diff := now.Sub(parsedTime); diff < -timeWindow || diff > timeWindow {
			xhttp.WriteAWSErr(w, r, xhttp.ErrRequestExpired)
			return
		}

		// 4. 构建规范请求
		canonicalRequest, payloadHash, err := buildCanonicalRequest(r, signedHeadersStr)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to build canonical request: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
			return
		}

		// 5. 构建字符串到签
		stringToSign := buildStringToSign(amzDate, date, region, service, canonicalRequest)

		// 6. 计算签名
		// 获取秘密访问密钥
		iamService := iam.GetIamService()
		if iamService == nil {
			logger.GetLogger("boulder").Errorf("Failed to get IAM service")
			xhttp.WriteAWSErr(w, r, xhttp.ErrServerNotInitialized)
			return
		}

		ak, err := iamService.GetAccessKey(accessKeyID)
		if ak == nil || err != nil {
			logger.GetLogger("boulder").Errorf("get access key failed: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidAccessKeyID)
			return
		}

		if ak.Status != "Active" || ak.ExpiredAt.Before(time.Now()) {
			logger.GetLogger("boulder").Errorf("access key is inactive: %v", err)
			xhttp.WriteAWSErr(w, r, xhttp.ErrAccessKeyDisabled)
			return
		}

		// 计算签名
		computedSignature := calculateSignature(ak.SecretAccessKey, date, region, service, stringToSign)

		// 在错误响应中使用 Request ID
		if !hmac.Equal([]byte(computedSignature), []byte(signature)) {
			logger.GetLogger("boulder").Warnf("signature mismatch %s : %s with ak %v ", computedSignature, signature, ak)
			xhttp.WriteAWSErr(w, r, xhttp.ErrSignatureDoesNotMatch)
			return
		}

		// 8. 验证内容哈希（如果使用UNSIGNED-PAYLOAD则跳过）
		if payloadHash != "UNSIGNED-PAYLOAD" {
			// 重新计算请求体哈希进行验证
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
				return
			}
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

			hash := sha256.Sum256(bodyBytes)
			calculatedHash := hex.EncodeToString(hash[:])

			if calculatedHash != payloadHash {
				xhttp.WriteAWSErr(w, r, xhttp.ErrContentChecksumMismatch)
				return
			}
		}
		// 签名验证成功，将解析的变量添加到请求上下文
		ctx := r.Context()
		ctx = context.WithValue(ctx, "accesskey", accessKeyID)
		ctx = context.WithValue(ctx, "region", region)

		// 签名验证成功，继续处理请求
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// 辅助函数：构建规范请求
func buildCanonicalRequest(r *http.Request, signedHeadersStr string) (string, string, error) {
	// 1. 请求方法
	method := r.Method

	// 2. 规范URI (双重编码)
	canonicalURI := encodePath(r.URL.Path)

	// 3. 规范查询字符串
	canonicalQueryString := utils.QueryEncode(r.URL.Query())

	// 4. 规范请求头
	canonicalHeaders, err := buildCanonicalHeaders(r, signedHeadersStr)
	if err != nil {
		return "", "", err
	}

	// 5. 签名头
	signedHeaders := strings.ToLower(signedHeadersStr)

	// 6. 有效载荷哈希
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	var bodyBytes []byte
	var errRead error

	if payloadHash == "" {
		// 对于GET、HEAD、DELETE等没有请求体的方法，使用UNSIGNED-PAYLOAD
		if r.Method == "GET" || r.Method == "HEAD" || r.Method == "DELETE" || r.Method == "OPTIONS" {
			payloadHash = "UNSIGNED-PAYLOAD"
		} else {
			// 读取请求体
			bodyBytes, errRead = io.ReadAll(r.Body)
			if errRead != nil {
				return "", "", errors.New("failed to read request body")
			}
			// 重置请求体，以便后续处理
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

			hash := sha256.Sum256(bodyBytes)
			payloadHash = hex.EncodeToString(hash[:])
		}
	} else if payloadHash == "UNSIGNED-PAYLOAD" {
		// 无需处理
	} else {
		// 对于提供了哈希值的情况，只需重置请求体
		bodyBytes, errRead = io.ReadAll(r.Body)
		if errRead != nil {
			return "", "", errors.New("failed to read request body")
		}
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	// 构建规范请求
	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	return canonicalRequest, payloadHash, nil
}

// 构建规范头
func buildCanonicalHeaders(r *http.Request, signedHeadersStr string) (string, error) {
	signedHeaders := strings.Split(strings.ToLower(signedHeadersStr), ";")
	sort.Strings(signedHeaders)

	var buf strings.Builder
	for _, h := range signedHeaders {
		values := r.Header.Values(h)
		if len(values) == 0 {
			values = []string{""}
		}

		combined := ""
		for i, v := range values {
			if i > 0 {
				combined += ","
			}

			v = strings.TrimSpace(v)
			v = spaceRegex.ReplaceAllString(v, " ")
			combined += v
		}

		buf.WriteString(h)
		buf.WriteByte(':')
		buf.WriteString(combined)
		buf.WriteByte('\n')
	}
	return buf.String(), nil
}

// 辅助函数：构建字符串到签
func buildStringToSign(amzDate, date, region, service, canonicalRequest string) string {
	stringToSign := "AWS4-HMAC-SHA256\n" +
		amzDate + "\n" +
		date + "/" + region + "/" + service + "/aws4_request\n"
	//logger.GetLogger("boulder").Infof("CANONICAL REQUEST:\n%s", canonicalRequest)
	hash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign += hex.EncodeToString(hash[:])
	//logger.GetLogger("boulder").Infof("STRING TO SIGN:\n%s", stringToSign)
	return stringToSign
}

// 辅助函数：计算签名
func calculateSignature(secretAccessKey, date, region, service, stringToSign string) string {
	// 派生签名密钥
	kDate := utils.HmacSHA256([]byte("AWS4"+secretAccessKey), date)
	kRegion := utils.HmacSHA256(kDate, region)
	kService := utils.HmacSHA256(kRegion, service)
	kSigning := utils.HmacSHA256(kService, "aws4_request")

	return hex.EncodeToString(utils.HmacSHA256(kSigning, stringToSign))
}

// 双重编码URI路径 (符合AWS规范)
func encodePath(path string) string {
	if path == "" {
		return "/"
	}

	encoded := utils.EncodePath(path)

	// 确保路径以斜杠开头
	if !strings.HasPrefix(encoded, "/") {
		encoded = "/" + encoded
	}
	return encoded
}
