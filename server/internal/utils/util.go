package utils

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"github.com/mageg-x/boulder/internal/logger"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/pbkdf2"
)

// TrimLeadingSlash 去前导 /   合并多个 /  处理 . 和 ..   规范化路径
func TrimLeadingSlash(ep string) string {
	if len(ep) > 0 && ep[0] == '/' {
		// Path ends with '/' preserve it
		if ep[len(ep)-1] == '/' && len(ep) > 1 {
			ep = path.Clean(ep)
			ep += "/"
		} else {
			ep = path.Clean(ep)
		}
		ep = ep[1:]
	}
	return ep
}

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

func GenUUID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

func GenKey(password string, keyLen int) []byte {
	// 实际应用中，salt 应随机生成并随密文一起存储
	salt := []byte("liusiming@rao") // 至少 8 字节

	// 迭代次数（建议 100,000 以上）
	iterations := 100

	key := pbkdf2.Key([]byte(password), salt, iterations, keyLen, sha256.New)
	return key
}

// Compress 压缩函数 - 使用Zstd
// Compress 压缩函数 - 使用Zstd
func Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed) // 直接传 buffer
	if err != nil {
		return nil, err
	}

	// 写入数据
	_, err = encoder.Write(data)
	if err != nil {
		encoder.Close() // 尽量关闭
		return nil, err
	}

	//关键：必须 Close 才会 flush 剩余数据
	err = encoder.Close()
	if err != nil {
		return nil, err
	}

	// 此时 compressed 里才有完整数据
	return compressed.Bytes(), nil
}

// Decompress 解压缩函数 - 使用Zstd
func Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	var decompressed bytes.Buffer
	decoder.Reset(bytes.NewReader(data))
	_, err = io.Copy(&decompressed, decoder)
	if err != nil {
		return nil, err
	}

	return decompressed.Bytes(), nil
}

// IsCompressibleZstd 判断数据是否值得用 zstd 压缩
func IsCompressible(data []byte, sampleSize int, thresholdRatio float64) bool {
	if len(data) == 0 {
		return false
	}

	// 只采样前 N 字节
	sample := data
	if len(data) > sampleSize {
		sample = data[:sampleSize]
	}

	// 使用最快的压缩等级进行试探
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		logger.GetLogger("boulder").Errorf("zstd encoder create failed: %v, assuming compressible", err)
		return false // 出错时保守压缩
	}

	compressed := encoder.EncodeAll(sample, nil)
	encoder.Close()

	originalSize := len(sample)
	compressedSize := len(compressed)

	if originalSize == 0 {
		return false
	}

	ratio := float64(compressedSize) / float64(originalSize)

	// ratio > threshold → 压缩效果差 → 不值得压缩
	return ratio < thresholdRatio
}

// Encrypt 加密函数 - 使用AES-GCM
func Encrypt(data []byte, key string) ([]byte, error) {
	keyBytes := GenKey(key, 16)

	// 创建 AES cipher
	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return nil, err
	}

	// 创建 GCM 模式
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// 生成随机 nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// 加密
	ciphertext := gcm.Seal(nonce, nonce, data, nil)

	return ciphertext, nil
}

// Decrypt  解密函数
func Decrypt(data []byte, key string) ([]byte, error) {
	keyBytes := GenKey(key, 16)

	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, encryptedData := data[:nonceSize], data[nonceSize:]

	// 解密（自动验证认证标签）
	plaintext, err := gcm.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return nil, errors.New("decryption failed: invalid key or corrupted data")
	}

	return plaintext, nil
}
