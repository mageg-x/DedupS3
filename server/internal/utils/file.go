package utils

import (
	"bufio"
	"io/fs"
	"os"
)

// WriteFile 专用的带缓冲写入函数
func WriteFile(filename string, chunks [][]byte, perm fs.FileMode) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// 动态计算缓冲区大小
	calculateBufferSize := func(dataSize int) int {
		// 根据数据大小选择缓冲区
		switch {
		case dataSize <= 64*1024: // <= 64KB
			return 64 * 1024
		case dataSize <= 1024*1024: // <= 1MB
			return 1024 * 1024
		case dataSize <= 16*1024*1024: // <= 16MB
			return 6 * 1024 * 1024
		case dataSize <= 64*1024*1024: // <= 64MB
			return 8 * 1024 * 1024 // 改为 8MB 或 16MB 即可
		default: // > 64MB
			return 8 * 1024 * 1024 // 改为 8MB 或 16MB 即可
		}
	}
	total := 0
	for _, chunk := range chunks {
		total += len(chunk)
	}
	// 根据数据大小动态调整缓冲区大小
	bufferSize := calculateBufferSize(total)
	writer := bufio.NewWriterSize(file, bufferSize)

	// 写入数据
	for _, chunk := range chunks {
		if len(chunk) == 0 {
			continue
		}
		if _, err := writer.Write(chunk); err != nil {
			return err
		}
	}

	// 刷新缓冲区
	if err := writer.Flush(); err != nil {
		return err
	}

	// 可选：关键数据启用 Sync
	// if err := file.Sync(); err != nil {
	//     return err
	// }
	return nil
}
