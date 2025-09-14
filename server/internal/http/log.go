package http

import (
	"net/http"

	"github.com/mageg-x/boulder/internal/logger"
)

// HttpLoggingTransport 实现 http.RoundTripper
type HttpLoggingTransport struct {
	Transport http.RoundTripper
}

func (t *HttpLoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// 打印请求头
	logger.GetLogger("boulder").Debugf("→ Request: %s %s", req.Method, req.URL)
	logger.GetLogger("boulder").Debugf("→ Headers:")
	for k, v := range req.Header {
		logger.GetLogger("boulder").Debugf("  %s: %v", k, v)
	}

	// 执行请求
	resp, err := t.Transport.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	// 打印响应头（可选）
	logger.GetLogger("boulder").Debugf("← Response: %d %s", resp.StatusCode, req.URL)
	logger.GetLogger("boulder").Debugf("← Headers:")
	for k, v := range resp.Header {
		logger.GetLogger("boulder").Debugf("  %s: %v", k, v)
	}

	return resp, nil
}
