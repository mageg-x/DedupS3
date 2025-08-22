package utils

import (
	"crypto/hmac"
	"crypto/sha256"
)

func HmacSHA256(key []byte, data string) []byte {
	hasher := hmac.New(sha256.New, key)
	hasher.Write([]byte(data))
	return hasher.Sum(nil)
}
