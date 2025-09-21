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

package meta

import (
	"fmt"

	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/storage/block"
)

const (
	DISK_TYPE_STORAGE = "disk"
	S3_TYPE_STORAGE   = "s3"

	// STANDARD 是 S3 的标准存储类，适用于频繁访问的数据。
	// 特点：
	//   - 高可用性（99.99%）
	//   - 高持久性（11 个 9）
	//   - 无取回费用
	//   - 数据跨多个可用区（AZ）冗余存储
	// 适用场景：网站内容、实时日志、大数据分析、应用程序数据
	STANDARD_CLASS_STORAGE = "STANDARD"

	// STANDARD_IA（Standard - Infrequent Access）
	// 标准不频繁访问存储类，适用于不常访问但需要快速获取的数据。
	// 特点：
	//   - 存储成本低于 STANDARD
	//   - 有数据取回费用
	//   - 最小存储持续时间为 30 天
	//   - 最小计量大小为 128 KB
	//   - 数据跨多个可用区冗余
	// 适用场景：备份、灾难恢复、不常访问的文件
	STANDARD_IA_CLASS_STORAGE = "STANDARD_IA"

	// ONEZONE_IA（One Zone - Infrequent Access）
	// 单可用区不频繁访问存储类，成本更低，但容错性较弱。
	// 特点：
	//   - 数据仅存储在一个可用区内（非跨区冗余）
	//   - 成本比 STANDARD_IA 更低
	//   - 若该可用区彻底损毁，数据将丢失
	//   - 有取回费用，最小存储 30 天
	// 适用场景：可容忍 AZ 故障的非关键数据、衍生数据
	ONEZONE_IA_CLASS_STORAGE = "ONEZONE_IA"

	// INTELLIGENT_TIERING（S3 Intelligent-Tiering）
	// 智能分层存储类，根据访问模式自动在热/冷层之间迁移。
	// 特点：
	//   - 无取回费用
	//   - 自动监控访问频率并迁移数据
	//   - 可配置监控到 Glacier 或 Deep Archive 的归档层
	// 适用场景：访问模式不确定的数据、希望自动优化成本的用户
	INTELLIGENT_TIERING_CLASS_STORAGE = "INTELLIGENT_TIERING"

	// GLACIER_IR（Glacier Instant Retrieval）
	// Glacier 即时恢复存储类，适用于需要毫秒级访问的归档数据。
	// 原名 Glacier（2023 年后更名为 Glacier Flexible Retrieval / IR）
	// 特点：
	//   - 归档存储，成本低
	//   - 支持毫秒级访问，无需 restore
	//   - 最小存储 90 天
	// 适用场景：需要快速访问的长期备份、合规归档
	GLACIER_IR_CLASS_STORAGE = "GLACIER_IR"

	// DEEP_ARCHIVE（S3 Glacier Deep Archive）
	// 深度归档存储类，成本最低，适合极少访问的数据。
	// 特点：
	//   - 检索时间通常为 12 小时起
	//   - 最小存储 180 天
	//   - 最适合长期保留（如 7-10 年以上）
	// 适用场景：法律合规归档、医疗记录、金融审计数据
	DEEP_ARCHIVE_CLASS_STORAGE = "DEEP_ARCHIVE"

	// OUTPOSTS（S3 on Outposts）
	// 用于 AWS Outposts 环境，数据存储在本地数据中心。
	// 特点：
	//   - 数据保留在客户本地设施中
	//   - 使用与云中 S3 相同的 API
	// 适用场景：本地低延迟访问、数据驻留要求
	OUTPOSTS_CLASS_STORAGE = "OUTPOSTS"

	// GLACIER_DIRECT_RETRIEVAL（Glacier 直接检索）
	// 新型 Glacier 存储类，支持直接访问而无需 restore 操作。
	// 简化了归档数据的访问流程，适合希望避免 restore 延迟的用户。
	// 注意：部分区域支持，是 Glacier IR 的进一步优化
	GLACIER_DIRECT_RETRIEVAL_CLASS_STORAGE = "GLACIER_DIRECT_RETRIEVAL"
)

// Storage 表示单个存储实例的元数据
type Storage struct {
	ID       string               `json:"id" msgpack:"id"`       // 唯一标识符
	Class    string               `json:"class" msgpack:"class"` // 存储类型 (标准， 低频， 归档存储)
	Type     string               `json:"type" msgpack:"type"`   // 存储类别 (s3, disk, etc.)
	Conf     config.StorageConfig `json:"conf" msgpack:"conf"`   // 存储配置
	Instance block.BlockStore     `json:"-" msgpack:"-"`         // 实际读写实例
}

func (s *Storage) String() string {
	return fmt.Sprintf("Storage{ID: %s, Class: %s, Type: %s, Conf: %+v}", s.ID, s.Class, s.Type, s.Conf)
}
