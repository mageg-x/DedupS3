package middleware

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/meta"
)

// 错误定义
var (
	ErrAccessKeyNotFound = errors.New("access key not found")

	spaceRegex = regexp.MustCompile(`\s+`)
)

// AWS4SigningMiddleware 提供AWS4签名验证的中间件
func AWS4SigningMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 获取请求上下文中的 Request ID
		requestID := GetRequestID(r.Context())

		// 1. 从请求头中提取签名信息
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			awsError(w, r, "MissingAuthenticationToken", "Missing Authorization header", http.StatusUnauthorized)
			return
		}

		// 解析Authorization头
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "AWS4-HMAC-SHA256" {
			awsError(w, r, "InvalidAuthentication", "Authorization header must start with AWS4-HMAC-SHA256", http.StatusUnauthorized)
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
			awsError(w, r, "InvalidArgument", "Authorization header missing required components", http.StatusBadRequest)
			return
		}

		// 提取访问密钥ID
		credentialParts := strings.Split(credential, "/")
		if len(credentialParts) < 5 {
			awsError(w, r, "InvalidArgument", "Credential format error", http.StatusBadRequest)
			return
		}

		accessKeyID := credentialParts[0]
		date := credentialParts[1]
		region := credentialParts[2]
		service := credentialParts[3]

		// 2. 查找对应的用户
		user, err := findUserByAccessKeyID(iamSystem, accessKeyID)
		if err != nil {
			if errors.Is(err, ErrAccessKeyNotFound) {
				awsError(w, r, "InvalidAccessKeyId", "The access key ID provided does not exist", http.StatusForbidden)
			} else {
				awsError(w, r, "InternalError", "Internal server error", http.StatusInternalServerError)
			}
			return
		}

		// 3. 检查时间有效性
		amzDate := r.Header.Get("X-Amz-Date")
		if amzDate == "" {
			awsError(w, r, "MissingDateHeader", "X-Amz-Date header is required", http.StatusBadRequest)
			return
		}

		// 验证时间格式
		if len(amzDate) != 16 {
			awsError(w, r, "MalformedDate", "Invalid X-Amz-Date format", http.StatusBadRequest)
			return
		}

		parsedTime, err := time.Parse("20060102T150405Z", amzDate)
		if err != nil {
			awsError(w, r, "MalformedDate", "Invalid X-Amz-Date format", http.StatusBadRequest)
			return
		}

		// 时间窗口：15分钟
		timeWindow := 15 * time.Minute
		now := time.Now().UTC()
		if diff := now.Sub(parsedTime); diff < -timeWindow || diff > timeWindow {
			awsError(w, r, "RequestExpired", "Request has expired", http.StatusForbidden)
			return
		}

		// 4. 构建规范请求
		canonicalRequest, payloadHash, err := buildCanonicalRequest(r, signedHeadersStr)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to build canonical request: %v", err)
			awsError(w, r, "InternalError", "Internal server error", http.StatusInternalServerError)
			return
		}

		// 5. 构建字符串到签
		stringToSign := buildStringToSign(amzDate, date, region, service, canonicalRequest)

		// 6. 计算签名
		// 获取秘密访问密钥
		secretAccessKey := ""
		for _, key := range user.AccessKeys {
			if key.AccessKeyID == accessKeyID && key.Status == "Active" {
				secretAccessKey = key.SecretAccessKey
				break
			}
		}

		if secretAccessKey == "" {
			awsError(w, r, "InvalidAccessKeyId", "The access key ID provided is inactive", http.StatusForbidden)
			return
		}

		// 计算签名
		computedSignature := calculateSignature(secretAccessKey, date, region, service, stringToSign)

		// 在错误响应中使用 Request ID
		if !hmac.Equal([]byte(computedSignature), []byte(signature)) {
			logger.GetLogger("boulder").WithFields(map[string]interface{}{
				"access_key_id": accessKeyID,
				"request_id":    requestID,
			}).Warn("Signature mismatch")

			awsError(w, r, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided", http.StatusForbidden)
			return
		}

		// 8. 验证内容哈希（如果使用UNSIGNED-PAYLOAD则跳过）
		if payloadHash != "UNSIGNED-PAYLOAD" {
			// 重新计算请求体哈希进行验证
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				awsError(w, r, "InternalError", "Failed to read request body", http.StatusInternalServerError)
				return
			}
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

			hash := sha256.Sum256(bodyBytes)
			calculatedHash := hex.EncodeToString(hash[:])

			if calculatedHash != payloadHash {
				awsError(w, r, "InvalidContentHash", "The provided content hash does not match the calculated content hash", http.StatusBadRequest)
				return
			}
		}

		// 签名验证成功，继续处理请求
		next.ServeHTTP(w, r)
	})
}

// 辅助函数：通过访问密钥ID查找用户
func findUserByAccessKeyID(iamSystem *meta.IAMSystem, accessKeyID string) (*meta.IAMUser, error) {
	for _, user := range iamSystem.Users {
		for _, key := range user.AccessKeys {
			if key.AccessKeyID == accessKeyID {
				return user, nil
			}
		}
	}
	return nil, ErrAccessKeyNotFound
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
		"", // 空行在规范头后面
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
		// 获取原始头值
		values := r.Header.Values(h)
		if len(values) == 0 {
			// 如果头不存在，使用空字符串
			values = []string{""}
		}

		// 合并多个值为逗号分隔，并规范化空格
		combined := ""
		for i, v := range values {
			if i > 0 {
				combined += ","
			}
			// 去除前后空格并将连续空格替换为单个空格
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

	hash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign += hex.EncodeToString(hash[:])

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

	// 使用自定义EncodePath函数进行双重编码
	encoded := utils.EncodePath(utils.EncodePath(path))

	// 确保路径以斜杠开头
	if !strings.HasPrefix(encoded, "/") {
		encoded = "/" + encoded
	}
	return encoded
}

// 生成AWS风格错误响应
func awsError(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)

	// 尝试从上下文中获取 Request ID
	requestID := GetRequestID(r.Context())
	if requestID == "" {
		// 如果没有设置，生成一个临时 ID
		requestID = fmt.Sprintf("TMP-%d", time.Now().UnixNano())
	}

	// 设置响应头
	w.Header().Set("x-amz-request-id", requestID)
	response := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>
		<Error>
			<Code>%s</Code>
			<Message>%s</Message>
			<RequestId>%s</RequestId>
		</Error>`,
		code, message, requestID,
	)

	w.Write([]byte(response))
}
