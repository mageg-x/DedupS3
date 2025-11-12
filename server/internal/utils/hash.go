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
package utils

import (
	"crypto/hmac"
	"crypto/sha256"
	"math/rand"
	"time"
)

// 先在 init 或程序启动时设置种子（只需一次）
func init() {
	rand.Seed(time.Now().UnixNano()) // Go 1.20+ 可省略，但建议保留
}

func HmacSHA256(key []byte, data string) []byte {
	hasher := hmac.New(sha256.New, key)
	hasher.Write([]byte(data))
	return hasher.Sum(nil)
}

func RandString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
