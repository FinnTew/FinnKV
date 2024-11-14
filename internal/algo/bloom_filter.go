package algo

import (
	"hash/fnv"
	"math"
	"sync"

	"github.com/spaolacci/murmur3"
)

// BloomFilter 布隆过滤器结构体
type BloomFilter struct {
	m    uint       // 位数组大小
	k    uint       // 哈希函数个数
	bits []byte     // 位数组
	lock sync.Mutex // 互斥锁，保证线程安全
}

// NewBloomFilter 创建一个新的布隆过滤器
func NewBloomFilter(n uint, p float64) *BloomFilter {
	m := optimalM(n, p)
	k := optimalK(n, m)

	// 初始化位数组
	bits := make([]byte, m/8+1)

	return &BloomFilter{
		m:    m,
		k:    k,
		bits: bits,
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
	bf.lock.Lock()
	defer bf.lock.Unlock()

	hashValues := make([]uint, bf.k)

	// 第一个哈希函数：FNV
	h1 := fnv.New64()
	h1.Write(data)
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

// Add 向布隆过滤器中添加元素
func (bf *BloomFilter) Add(data []byte) {
	hashValues := bf.hashFunctions(data)
	for _, hv := range hashValues {
		byteIndex := hv / 8
		bitIndex := hv % 8
		bf.bits[byteIndex] |= 1 << bitIndex
	}
}

// Contains 检查元素是否可能存在于布隆过滤器中
func (bf *BloomFilter) Contains(data []byte) bool {
	hashValues := bf.hashFunctions(data)
	for _, hv := range hashValues {
		byteIndex := hv / 8
		bitIndex := hv % 8
		if bf.bits[byteIndex]&(1<<bitIndex) == 0 {
			return false
		}
	}
	return true
}
