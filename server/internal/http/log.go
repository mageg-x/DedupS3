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
package http

import (
	"net/http"

	"github.com/mageg-x/dedups3/internal/logger"
)

// HttpLoggingTransport 实现 http.RoundTripper
type HttpLoggingTransport struct {
	Transport http.RoundTripper
}

func (t *HttpLoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// 打印请求头
	logger.GetLogger("dedups3").Debugf("→ Request: %s %s", req.Method, req.URL)
	logger.GetLogger("dedups3").Debugf("→ Headers:")
	for k, v := range req.Header {
		logger.GetLogger("dedups3").Debugf("  %s: %v", k, v)
	}

	// 执行请求
	resp, err := t.Transport.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	// 打印响应头（可选）
	logger.GetLogger("dedups3").Debugf("← Response: %d %s", resp.StatusCode, req.URL)
	logger.GetLogger("dedups3").Debugf("← Headers:")
	for k, v := range resp.Header {
		logger.GetLogger("dedups3").Debugf("  %s: %v", k, v)
	}

	return resp, nil
}
