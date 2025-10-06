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
