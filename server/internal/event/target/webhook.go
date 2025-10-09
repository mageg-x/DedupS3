package target

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/utils"
)

var (
	ErrNotConnected = errors.New("not connected to target server/service")
)

type WebhookArgs struct {
	TargetArgHead
	Endpoint  url.URL `json:"endpoint"`
	AuthToken string  `json:"authToken"`
}

// 添加连接状态管理
type webhookConnectionState struct {
	httpClient *http.Client
	mu         sync.RWMutex
}

// WebhookTarget 优化后的 WebhookTarget 结构
type WebhookTarget struct {
	initOnce   utils.Init
	args       WebhookArgs
	addr       string
	state      *webhookConnectionState
	cancel     context.CancelFunc
	cancelCh   <-chan struct{}
	maxRetries int
	retryDelay time.Duration
	mu         sync.RWMutex
	timeout    time.Duration
	userAgent  string
}

// NewWebhookTarget - creates new Webhook target.
func NewWebhookTarget(ctx context.Context, args WebhookArgs) (*WebhookTarget, error) {
	_ctx, cancel := context.WithCancel(ctx)

	target := &WebhookTarget{
		args:     args,
		cancel:   cancel,
		cancelCh: _ctx.Done(),
	}

	// Calculate the webhook addr with the port number format
	addr := args.Endpoint.Host
	if _, _, err := net.SplitHostPort(args.Endpoint.Host); err != nil && strings.Contains(err.Error(), "missing port in address") {
		switch strings.ToLower(args.Endpoint.Scheme) {
		case "http":
			addr += ":80"
		case "https":
			addr += ":443"
		default:
			return nil, errors.New("unsupported scheme")
		}
	}
	target.addr = addr
	return target, nil
}

func (target *WebhookTarget) ID() string {
	return target.args.ID
}

func (target *WebhookTarget) Arn() string {
	return target.args.Arn
}

func (target *WebhookTarget) Owner() string {
	return target.args.Owner
}

func (target *WebhookTarget) Type() string {
	return EVENT_TARGET_TYPE_WEBHOOK
}

func (target *WebhookTarget) Init() error {
	return target.initOnce.Do(func() error {
		target.state = &webhookConnectionState{}
		target.maxRetries = 3
		target.retryDelay = 2 * time.Second
		target.timeout = 30 * time.Second
		target.userAgent = "Boulder-Webhook-Target/1.0"
		return target.setupHTTPClient()
	})
}

func (target *WebhookTarget) IsActive() (bool, error) {
	if err := target.Init(); err != nil {
		return false, err
	}

	target.state.mu.RLock()
	defer target.state.mu.RUnlock()

	select {
	case <-target.cancelCh:
		return false, errors.New("webhook target is closed")
	default:
	}

	// 使用更快速的 TCP 连接检查
	if err := target.checkTCPConnection(); err != nil {
		return false, err
	}

	return true, nil
}

func (target *WebhookTarget) Send(eventData Event) error {
	if err := target.Init(); err != nil {
		return err
	}

	// 序列化事件数据
	data, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	// 带重试的发送
	return target.sendWithRetry(data)
}

func (target *WebhookTarget) Close() error {
	target.cancel()

	target.mu.Lock()
	defer target.mu.Unlock()

	return target.cleanup()
}

// GetArg 返回Webhook目标的参数
func (target *WebhookTarget) GetArg() (interface{}, error) {
	return target.args, nil
}

func (target *WebhookTarget) setupHTTPClient() error {
	target.state.mu.Lock()
	defer target.state.mu.Unlock()

	// 创建自定义传输层，优化性能
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// 创建 HTTP 客户端
	target.state.httpClient = &http.Client{
		Transport: &xhttp.HttpLoggingTransport{
			Transport: transport,
		},
		Timeout: target.timeout,
	}

	// 验证端点可用性
	return target.verifyEndpoint()
}

func (target *WebhookTarget) verifyEndpoint() error {
	// 先进行 TCP 连接检查
	if err := target.checkTCPConnection(); err != nil {
		return fmt.Errorf("TCP connection failed: %w", err)
	}

	// 再进行 HTTP 级别检查（可选）
	if err := target.checkHTTPEndpoint(); err != nil {
		return fmt.Errorf("HTTP endpoint verification failed: %w", err)
	}

	return nil
}

func (target *WebhookTarget) checkTCPConnection() error {
	conn, err := net.DialTimeout("tcp", target.addr, 5*time.Second)
	if err != nil {
		if utils.IsNetworkOrHostDown(err, false) {
			return ErrNotConnected
		}
		return fmt.Errorf("TCP dial failed: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	return nil
}

func (target *WebhookTarget) checkHTTPEndpoint() error {
	// 发送一个轻量级的 HEAD 请求检查端点
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "HEAD", target.args.Endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create HEAD request: %w", err)
	}

	target.setAuthHeaders(req)

	resp, err := target.state.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HEAD request failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// 检查状态码
	if resp.StatusCode >= 400 {
		return fmt.Errorf("endpoint returned status: %s", resp.Status)
	}

	return nil
}

func (target *WebhookTarget) sendWithRetry(data []byte) error {
	var lastErr error

	for i := 0; i < target.maxRetries; i++ {
		select {
		case <-target.cancelCh:
			return errors.New("target closed during send")
		default:
		}

		if err := target.trySend(data); err != nil {
			lastErr = err

			// 如果是临时错误，等待后重试
			if isTemporaryError(err) {
				time.Sleep(target.retryDelay * time.Duration(i+1))
				continue
			}
			// 永久错误直接返回
			return err
		}
		return nil
	}

	return fmt.Errorf("failed to send webhook after %d attempts: %w", target.maxRetries, lastErr)
}

func (target *WebhookTarget) trySend(data []byte) error {
	target.state.mu.RLock()
	defer target.state.mu.RUnlock()

	if target.state.httpClient == nil {
		return errors.New("http client not available")
	}

	// 创建带有上下文的请求
	ctx, cancel := context.WithTimeout(context.Background(), target.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.args.Endpoint.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// 设置请求头
	target.setRequestHeaders(req)

	resp, err := target.state.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send webhook request: %w", err)
	}
	defer func() {
		if resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body) // 确保读取完响应体
			_ = resp.Body.Close()
		}
	}()

	// 检查响应状态码
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}

	// 处理错误响应
	return target.handleErrorResponse(resp)
}

func (target *WebhookTarget) setRequestHeaders(req *http.Request) {
	// 设置认证头部
	target.setAuthHeaders(req)

	// 设置通用头
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", target.userAgent)
	req.Header.Set("X-Request-ID", generateRequestID())
	req.Header.Set("Date", time.Now().Format(time.RFC1123))
}

func (target *WebhookTarget) setAuthHeaders(req *http.Request) {
	if target.args.AuthToken == "" {
		return
	}

	tokens := strings.Fields(target.args.AuthToken)
	switch len(tokens) {
	case 2:
		// 已经是完整的 Authorization 头格式
		req.Header.Set("Authorization", target.args.AuthToken)
	case 1:
		// 只有 token，默认使用 Bearer
		req.Header.Set("Authorization", "Bearer "+target.args.AuthToken)
	default:
		// 其他情况直接设置
		req.Header.Set("Authorization", target.args.AuthToken)
	}
}

func generateRequestID() string {
	return fmt.Sprintf("%d-%x", time.Now().UnixNano(), rand.Int31())
}

func (target *WebhookTarget) handleErrorResponse(resp *http.Response) error {
	// 读取响应体
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024)) // 限制读取大小
	if err != nil {
		body = []byte("failed to read response body")
	}

	responseBody := string(body)
	if len(responseBody) >= 1024 {
		responseBody = responseBody[:1024] + "... (truncated)"
	}

	// 根据状态码返回具体错误
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return fmt.Errorf("webhook authentication failed (401): please check your auth token")
	case http.StatusForbidden:
		return fmt.Errorf("webhook access forbidden (403): insufficient permissions")
	case http.StatusNotFound:
		return fmt.Errorf("webhook endpoint not found (404): please check the URL")
	case http.StatusRequestTimeout:
		return fmt.Errorf("webhook request timeout (408)")
	case http.StatusTooManyRequests:
		retryAfter := resp.Header.Get("Retry-After")
		return fmt.Errorf("webhook rate limited (429): too many requests. retry after: %s", retryAfter)
	case http.StatusInternalServerError:
		return fmt.Errorf("webhook server error (500): %s", responseBody)
	case http.StatusBadGateway:
		return fmt.Errorf("webhook bad gateway (502)")
	case http.StatusServiceUnavailable:
		return fmt.Errorf("webhook service unavailable (503)")
	case http.StatusGatewayTimeout:
		return fmt.Errorf("webhook gateway timeout (504)")
	default:
		return fmt.Errorf("webhook request failed with status %d: %s", resp.StatusCode, responseBody)
	}
}

// 判断是否为临时错误
func isTemporaryError(err error) bool {
	if err == nil {
		return false
	}

	// 网络错误通常是临时的
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		utils.IsNetworkOrHostDown(err, false) {
		return true
	}

	// HTTP 错误码判断
	var httpErr interface {
		StatusCode() int
	}
	if errors.As(err, &httpErr) {
		statusCode := httpErr.StatusCode()
		// 5xx 错误和 429 通常是临时的
		return statusCode >= 500 || statusCode == http.StatusTooManyRequests
	}

	// 检查错误字符串
	errorStr := strings.ToLower(err.Error())
	temporaryErrors := []string{
		"timeout",
		"deadline exceeded",
		"connection refused",
		"connection reset",
		"temporary",
		"retry",
	}

	for _, tempErr := range temporaryErrors {
		if strings.Contains(errorStr, tempErr) {
			return true
		}
	}

	return false
}

func (target *WebhookTarget) cleanup() error {
	if target.state == nil {
		return nil
	}

	target.state.mu.Lock()
	defer target.state.mu.Unlock()

	if target.state.httpClient != nil {
		// 关闭空闲连接
		target.state.httpClient.CloseIdleConnections()
	}

	return nil
}
