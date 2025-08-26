package utils

import (
	"encoding/xml"
	"errors"
	"io"
	"sync"
)

// SliceDiff 返回两个差集：
// - onlyInA: 在 slice1 中有，但在 slice2 中没有的元素
// - onlyInB: 在 slice2 中有，但在 slice1 中没有的元素
// 使用泛型 T 和自定义比较函数 equal
func SliceDiff[T any](slice1, slice2 []T, equal func(T, T) bool) (onlyInA, onlyInB []T) {
	// 计算 slice1 相对于 slice2 的差集 (onlyInA)
	for _, v1 := range slice1 {
		found := false
		for _, v2 := range slice2 {
			if equal(v1, v2) {
				found = true
				break
			}
		}
		if !found {
			onlyInA = append(onlyInA, v1)
		}
	}

	// 计算 slice2 相对于 slice1 的差集 (onlyInB)
	for _, v2 := range slice2 {
		found := false
		for _, v1 := range slice1 {
			if equal(v1, v2) {
				found = true
				break
			}
		}
		if !found {
			onlyInB = append(onlyInB, v2)
		}
	}

	return onlyInA, onlyInB // Go 支持多返回值
}

// xmlDecoder provide decoded value in xml.
func XmlDecoder(body io.Reader, v interface{}, size int64) error {
	var lbody io.Reader
	if size > 0 {
		lbody = io.LimitReader(body, size)
	} else {
		lbody = body
	}
	d := xml.NewDecoder(lbody)
	// Ignore any encoding set in the XML body
	d.CharsetReader = func(label string, input io.Reader) (io.Reader, error) {
		return input, nil
	}
	err := d.Decode(v)
	if errors.Is(err, io.EOF) {
		err = &xml.SyntaxError{
			Line: 0,
			Msg:  err.Error(),
		}
	}
	return err
}

// WithLock 是一个辅助函数，用于在特定代码块内自动锁定和解锁
func WithLock(mu *sync.Mutex, fn func()) {
	mu.Lock()
	defer mu.Unlock()
	fn()
}

func WithTryLock(mu *sync.Mutex, fn func()) {
	if mu.TryLock() {
		defer mu.Unlock()
		fn()
	}
}
