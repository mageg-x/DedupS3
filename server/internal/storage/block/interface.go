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
	"context"
	"errors"
)

var (
	ErrBlockNotFound = errors.New("block not found")
)

// BlockStore  存储后端接口
type BlockStore interface {
	Type() string
	WriteBlock(ctx context.Context, blockID string, data []byte) error
	ReadBlock(blockID string, offset, length int64) ([]byte, error)
	DeleteBlock(blockID string) error
	List() (<-chan string, <-chan error)
	Location(blockID string) string
	BlockExists(blockID string) (bool, error)
}
