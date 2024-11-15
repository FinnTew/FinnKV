package algo

import (
	"hash/fnv"
	"math"
	"sync"

	"github.com/spaolacci/murmur3"
)

// BloomFilter 计数布隆过滤器结构体
type BloomFilter struct {
	m      uint       // 位数组大小
	k      uint       // 哈希函数个数
	counts []uint32   // 计数器数组
	lock   sync.Mutex // 互斥锁，保证线程安全
}

// NewBloomFilter 创建一个新的计数布隆过滤器
func NewBloomFilter(n uint, p float64) *BloomFilter {
	m := optimalM(n, p)
	k := optimalK(n, m)

	// 初始化计数器数组
	counts := make([]uint32, m)

	return &BloomFilter{
		m:      m,
		k:      k,
		counts: counts,
	}
}

// optimalM 计算最佳位数组大小 m
func optimalM(n uint, p float64) uint {
	m := -1 * float64(n) * math.Log(p) / (math.Pow(math.Log(2), 2))
	return uint(math.Ceil(m))
}

// optimalK 计算最佳哈希函数个数 k
func optimalK(n uint, m uint) uint {
	k := (float64(m) / float64(n)) * math.Log(2)
	return uint(math.Ceil(k))
}

// hashFunctions 生成哈希值列表，使用双哈希技术
func (bf *BloomFilter) hashFunctions(data []byte) []uint {
	hashValues := make([]uint, bf.k)

	// 第一个哈希函数：FNV
	h1 := fnv.New64()
	_, err := h1.Write(data)
	if err != nil {
		return nil
	}
	sum1 := h1.Sum64()

	// 第二个哈希函数：MurmurHash
	sum2 := murmur3.Sum64(data)

	for i := uint(0); i < bf.k; i++ {
		// 使用双哈希技术生成多个哈希值
		combinedHash := sum1 + uint64(i)*sum2
		hashValues[i] = uint(combinedHash % uint64(bf.m))
	}

	return hashValues
}

// Add 向计数布隆过滤器中添加元素
func (bf *BloomFilter) Add(data []byte) {
	hashValues := bf.hashFunctions(data)
	bf.lock.Lock()
	defer bf.lock.Unlock()

	for _, hv := range hashValues {
		bf.counts[hv]++
	}
}

// Contains 检查元素是否可能存在于计数布隆过滤器中
func (bf *BloomFilter) Contains(data []byte) bool {
	hashValues := bf.hashFunctions(data)
	bf.lock.Lock()
	defer bf.lock.Unlock()

	for _, hv := range hashValues {
		if bf.counts[hv] == 0 {
			return false
		}
	}
	return true
}

// Remove 从计数布隆过滤器中删除元素
func (bf *BloomFilter) Remove(data []byte) {
	hashValues := bf.hashFunctions(data)
	bf.lock.Lock()
	defer bf.lock.Unlock()

	for _, hv := range hashValues {
		if bf.counts[hv] > 0 {
			bf.counts[hv]--
		}
	}
}
