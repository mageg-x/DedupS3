package http

import (
	"bufio"
	"net"
	"net/http"
)

// responseWriter wraps http.ResponseWriter to capture status code and bytes written
type RespWriter struct {
	http.ResponseWriter
	statusCode int
	bytes      int64
}

// WriteHeader captures the status code
func (rw *RespWriter) WriteHeader(code int) {
	if rw.statusCode == 0 {
		rw.statusCode = code
	}
	rw.ResponseWriter.WriteHeader(code)
}

// Write captures the number of bytes written to the body
func (rw *RespWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytes += int64(n)
	return n, err
}

// StatusCode returns the HTTP status code (0 if not set, defaults to 200)
func (rw *RespWriter) StatusCode() int {
	if rw.statusCode == 0 {
		return http.StatusOK // default if WriteHeader not called
	}
	return rw.statusCode
}

// BytesWritten returns the number of response body bytes written
func (rw *RespWriter) BytesWritten() int64 {
	return rw.bytes
}

// Hijack implements http.Hijacker
func (rw *RespWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hijacker.Hijack()
}

// Flush implements http.Flusher
func (rw *RespWriter) Flush() {
	if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
