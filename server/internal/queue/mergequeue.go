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
package queue

import (
	"container/list"
	"sync"
)

type MQItem struct {
	Key   string
	Value interface{}
}

type MQueue struct {
	list  *list.List
	index map[string]*list.Element
	mu    sync.Mutex
	merge MergeFunc
}

type MergeFunc func(old, new *MQItem) *MQItem

func NewMergeQueue(merge MergeFunc) *MQueue {
	if merge == nil {
		merge = func(old, new *MQItem) *MQItem { return new }
	}
	return &MQueue{
		list:  list.New(),
		index: make(map[string]*list.Element),
		merge: merge,
	}
}

func (q *MQueue) Push(item *MQItem) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if elem, exists := q.index[item.Key]; exists {
		oldItem := elem.Value.(*MQItem)
		merged := q.merge(oldItem, item)
		elem.Value = merged
	} else {
		elem := q.list.PushBack(item)
		q.index[item.Key] = elem
	}
}

func (q *MQueue) Pop() *MQItem {
	q.mu.Lock()
	defer q.mu.Unlock()

	front := q.list.Front()
	if front == nil {
		return nil
	}

	q.list.Remove(front)
	item := front.Value.(*MQItem)
	delete(q.index, item.Key)

	return item
}

func (q *MQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.list.Len()
}
