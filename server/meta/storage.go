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
	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/storage/block"
)

// Storage 表示单个存储实例的元数据
type Storage struct {
	ID       string             `json:"id" msgpack:"id"`       // 唯一标识符
	Class    string             `json:"class" msgpack:"class"` // 存储类型 (标准， 低频， 归档存储)
	Type     string             `json:"type" msgpack:"type"`   // 存储类别 (s3, disk, etc.)
	Conf     config.BlockConfig `json:"conf" msgpack:"conf"`   // 存储配置
	Instance block.BlockStore   `json:"-" msgpack:"-"`         // 实际读写实例
}
