package algo

import (
	"math/rand"
	"sync"
)

const (
	MaxLevel    = 32
	Probability = 0.25
)

type Node[K comparable, V any] struct {
	key     K
	value   V
	forward []*Node[K, V]
}

func NewNode[K comparable, V any](key K, value V, level int) *Node[K, V] {
	return &Node[K, V]{
		key:     key,
		value:   value,
		forward: make([]*Node[K, V], level+1),
	}
}

type LessFunc[T comparable] func(a, b T) bool

type SkipList[K comparable, V any] struct {
	head   *Node[K, V]
	tail   *Node[K, V]
	level  int
	length int
	less   LessFunc[K]
	lock   sync.RWMutex
}

func NewSkipList[K comparable, V any](less LessFunc[K]) *SkipList[K, V] {
	head := NewNode(*new(K), *new(V), MaxLevel)
	tail := NewNode(*new(K), *new(V), MaxLevel)
	for i := range head.forward {
		head.forward[i] = tail
	}
	return &SkipList[K, V]{
		head:   head,
		tail:   tail,
		level:  0,
		length: 0,
		less:   less,
	}
}

func (s *SkipList[K, V]) randomLevel() int {
	level := 1
	for rand.Float64() < Probability && level < MaxLevel {
		level++
	}
	return level
}

func (s *SkipList[K, V]) Add(key K, value V) {
	s.lock.Lock()
	defer s.lock.Unlock()

	update := make([]*Node[K, V], MaxLevel)
	curr := s.head
	for i := s.level; i >= 0; i-- {
		for curr.forward[i] != s.tail && s.less(curr.forward[i].key, key) {
			curr = curr.forward[i]
		}
		update[i] = curr
	}
	curr = curr.forward[0]
	if curr != s.tail && curr.key == key {
		curr.value = value
		return
	}

	level := s.randomLevel()
	if level > s.level {
		for i := s.level + 1; i <= level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	newNode := NewNode(key, value, level)
	for i := 0; i <= level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
	s.length++
}

func (s *SkipList[K, V]) Find(key K) (V, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	curr := s.head
	for i := s.level; i >= 0; i-- {
		for curr.forward[i] != s.tail && s.less(curr.forward[i].key, key) {
			curr = curr.forward[i]
		}
	}
	curr = curr.forward[0]
	if curr != s.tail && curr.key == key {
		return curr.value, true
	}
	return *new(V), false
}

func (s *SkipList[K, V]) Del(key K) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	update := make([]*Node[K, V], MaxLevel)
	curr := s.head
	for i := s.level; i >= 0; i-- {
		for curr.forward[i] != s.tail && s.less(curr.forward[i].key, key) {
			curr = curr.forward[i]
		}
		update[i] = curr
	}
	curr = curr.forward[0]
	if curr == s.tail || curr.key != key {
		return false
	}

	for i := 0; i <= s.level; i++ {
		if update[i].forward[i] != curr {
			break
		}
		update[i].forward[i] = curr.forward[i]
	}

	for s.level > 0 && s.head.forward[s.level] == s.tail {
		s.level--
	}
	s.length--
	return true
}

func (s *SkipList[K, V]) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.length
}

func (s *SkipList[K, V]) Iterator() func() (K, V, bool) {
	s.lock.RLock()
	curr := s.head.forward[0]
	s.lock.RUnlock()

	return func() (K, V, bool) {
		s.lock.RLock()
		defer s.lock.RUnlock()

		if curr == s.tail {
			return *new(K), *new(V), false
		}
		key, value := curr.key, curr.value
		curr = curr.forward[0]
		return key, value, true
	}
}
