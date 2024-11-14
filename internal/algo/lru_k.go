package algo

import (
	"container/heap"
)

type CacheItem struct {
	Key      string
	Value    interface{}
	Accesses []int64
	index    int
}

func (item *CacheItem) Priority(k int) int64 {
	if len(item.Accesses) < k {
		return -1
	}
	return item.Accesses[0]
}

type PriorityQueue struct {
	items []*CacheItem
	k     int
}

func (pq PriorityQueue) Len() int { return len(pq.items) }

func (pq PriorityQueue) Less(i, j int) bool {
	priorityI := pq.items[i].Priority(pq.k)
	priorityJ := pq.items[j].Priority(pq.k)
	if priorityI == -1 && priorityJ == -1 {
		return pq.items[i].Accesses[len(pq.items[i].Accesses)-1] < pq.items[j].Accesses[len(pq.items[j].Accesses)-1]
	} else if priorityI == -1 {
		return true
	} else if priorityJ == -1 {
		return false
	} else {
		return priorityI < priorityJ
	}
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(pq.items)
	item := x.(*CacheItem)
	item.index = n
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	item.index = -1
	pq.items = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) update(item *CacheItem) {
	heap.Fix(pq, item.index)
}

type LRUKCache struct {
	capacity    int
	k           int
	items       map[string]*CacheItem
	accessCount int64
	pq          *PriorityQueue
}

func NewLRUKCache(capacity int, k int) *LRUKCache {
	pq := &PriorityQueue{
		items: []*CacheItem{},
		k:     k,
	}
	heap.Init(pq)
	return &LRUKCache{
		capacity: capacity,
		k:        k,
		items:    make(map[string]*CacheItem),
		pq:       pq,
	}
}

func (c *LRUKCache) Get(key string) (interface{}, bool) {
	c.accessCount++
	if item, ok := c.items[key]; ok {
		item.Accesses = append(item.Accesses, c.accessCount)
		if len(item.Accesses) > c.k {
			item.Accesses = item.Accesses[1:]
		}
		c.pq.update(item)
		return item.Value, true
	}
	return nil, false
}

func (c *LRUKCache) Set(key string, value interface{}) {
	c.accessCount++
	if item, ok := c.items[key]; ok {
		item.Value = value
		item.Accesses = append(item.Accesses, c.accessCount)
		if len(item.Accesses) > c.k {
			item.Accesses = item.Accesses[1:]
		}
		c.pq.update(item)
	} else {
		if len(c.items) >= c.capacity {
			evictedItem := heap.Pop(c.pq).(*CacheItem)
			delete(c.items, evictedItem.Key)
		}
		item := &CacheItem{
			Key:      key,
			Value:    value,
			Accesses: []int64{c.accessCount},
		}
		c.items[key] = item
		heap.Push(c.pq, item)
	}
}
