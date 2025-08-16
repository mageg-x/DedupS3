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
package block

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Store 实现基于S3的存储后端
type S3Store struct {
	Id       string
	Bucket   string
	Prefix   string
	client   *s3.Client
	region   string
	stats    StoreStats
	mu       sync.RWMutex
	lastStat time.Time
}

// NewS3Storage 创建新的S3存储后端
func InitS3Store(id, bucket, prefix, region string) (*S3Store, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)

	return &S3Store{
		Id:     id,
		Bucket: bucket,
		Prefix: prefix,
		client: client,
		region: region,
	}, nil
}

// ID 返回存储ID
func (s *S3Store) ID() string {
	return s.Id
}

// Type 返回存储类型
func (s *S3Store) Type() string {
	return "s3"
}

// WriteBlock 写入块到S3
func (s *S3Store) WriteBlock(blockID string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.blockKey(blockID)
	_, err := s.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})

	if err != nil {
		return err
	}

	s.updateStats()
	return nil
}

// ReadBlock 从S3读取块
func (s *S3Store) ReadBlock(blockID string, offset, length int64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.blockKey(blockID)

	// 处理范围请求
	rangeHeader := ""
	if length > 0 {
		rangeHeader = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	}

	if rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}

	resp, err := s.client.GetObject(context.TODO(), input)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

// DeleteBlock 删除S3块
func (s *S3Store) DeleteBlock(blockID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.blockKey(blockID)
	_, err := s.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return err
	}

	s.updateStats()
	return nil
}

// Location 获取块位置
func (s *S3Store) Location(blockID string) string {
	return fmt.Sprintf("s3://%s/%s", s.Bucket, s.blockKey(blockID))
}

// Stats 获取存储统计信息
func (s *S3Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 如果统计信息超过5分钟未更新，则更新
	if time.Since(s.lastStat) > 5*time.Minute {
		go s.updateStats()
	}

	return s.stats
}

// blockKey 获取块在S3中的键
func (s *S3Store) blockKey(blockID string) string {
	// 使用两级目录分散对象
	dir1 := blockID[:2]
	dir2 := blockID[2:4]
	return filepath.Join(s.Prefix, dir1, dir2, blockID)
}

// updateStats 更新S3存储统计信息
func (s *S3Store) updateStats() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取桶信息
	_, err := s.client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(s.Bucket),
	})

	if err != nil {
		return
	}

	// 获取桶大小（简化实现）
	// 实际实现中应使用S3清单或存储统计服务
	s.stats = StoreStats{
		TotalSpace: 1 * 1024 * 1024 * 1024 * 1024, // 1TB (简化)
		UsedSpace:  500 * 1024 * 1024 * 1024,      // 500GB (简化)
		FreeSpace:  500 * 1024 * 1024 * 1024,      // 500GB (简化)
	}

	// 标记更新时间
	s.lastStat = time.Now()
}
