package redis

type HashTable interface {
	HSet(field string, value []byte) bool
	HGet(field string) ([]byte, bool)
	HDel(fields ...string) int
	HGetAll() map[string][]byte
}

func NewHashTable() HashTable {
	return &hashTable{
		data: make(map[string][]byte),
	}
}

type hashTable struct {
	data map[string][]byte
}

func (h *hashTable) HSet(field string, value []byte) bool {
	_, exists := h.data[field]
	h.data[field] = value
	return !exists
}

func (h *hashTable) HGet(field string) ([]byte, bool) {
	val, exists := h.data[field]
	return val, exists
}

func (h *hashTable) HDel(fields ...string) int {
	deleted := 0
	for _, field := range fields {
		if _, exists := h.data[field]; exists {
			delete(h.data, field)
			deleted++
		}
	}
	return deleted
}

func (h *hashTable) HGetAll() map[string][]byte {
	// 返回数据的副本以防止外部修改
	copyData := make(map[string][]byte, len(h.data))
	for k, v := range h.data {
		copyData[k] = v
	}
	return copyData
}
