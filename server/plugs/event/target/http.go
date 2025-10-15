package target

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/plugs/event"
)

// HTTPTarget 实现EventTarget接口的HTTP版本
type HTTPTarget struct {
	serviceURL string
	authToken  string
	httpClient *http.Client
}

// NewHTTPTarget 创建一个HTTP事件目标实例
func NewHTTPTarget(serviceURL string, authToken string) (*HTTPTarget, error) {
	if serviceURL == "" {
		return nil, fmt.Errorf("http event target requires service URL")
	}

	// 创建HTTP客户端
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	return &HTTPTarget{
		serviceURL: serviceURL,
		authToken:  authToken,
		httpClient: httpClient,
	}, nil
}

// Send 通过HTTP发送单条事件日志
func (h *HTTPTarget) Send(ctx context.Context, record *event.Record) error {
	// 将record转换为JSON
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal event record: %w", err)
	}

	// 创建请求
	req, err := http.NewRequestWithContext(ctx, "POST", h.serviceURL+"/api/event/logs", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
	if h.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+h.authToken)
	}

	// 发送请求
	resp, err := h.httpClient.Do(req)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to send event log via HTTP: %v", err)
		return fmt.Errorf("failed to send event log: %w", err)
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("event service returned non-success status: %d", resp.StatusCode)
	}

	return nil
}

// Query 通过HTTP查询事件日志
func (h *HTTPTarget) Query(ctx context.Context, cond *QueryCondition, opts *QueryOption) (*QueryResult, error) {
	// 构建查询参数
	queryParams := make(map[string]interface{})

	// 添加查询条件
	if cond != nil {
		if cond.StartTime != nil {
			queryParams["start_time"] = cond.StartTime.Format(time.RFC3339)
		}

		if cond.EndTime != nil {
			queryParams["end_time"] = cond.EndTime.Format(time.RFC3339)
		}

		if cond.EventName != nil && *cond.EventName != "" {
			queryParams["event_name"] = *cond.EventName
		}

		if cond.UserID != nil && *cond.UserID != "" {
			queryParams["user_id"] = *cond.UserID
		}
	}

	// 添加查询选项
	if opts != nil {
		if opts.Offset >= 0 {
			queryParams["offset"] = opts.Offset
		}

		if opts.OrderBy != "" {
			queryParams["order_by"] = opts.OrderBy
		}
	}

	// 将查询参数转换为JSON
	paramsData, err := json.Marshal(queryParams)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query params: %w", err)
	}

	// 创建请求
	req, err := http.NewRequestWithContext(ctx, "POST", h.serviceURL+"/api/event/query", bytes.NewBuffer(paramsData))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
	if h.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+h.authToken)
	}

	// 发送请求
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send query request: %w", err)
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query request failed with status: %d", resp.StatusCode)
	}

	// 解析响应结果
	var result QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode query result: %w", err)
	}

	return &result, nil
}

// Close 关闭HTTP连接（当前HTTP客户端不需要特殊关闭操作）
func (h *HTTPTarget) Close() error {
	// HTTP客户端不需要特殊关闭操作
	return nil
}
