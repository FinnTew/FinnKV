package algo_test

import (
	"FinnKV/internal/algo"
	"testing"
	"time"
)

func TestLRUKCache(t *testing.T) {
	cache := algo.NewLRUKCache(3, 2)

	// 插入三个值
	cache.Set("a", 1)
	cache.Set("b", 2)
	cache.Set("c", 3)

	// 访问 "a" 两次
	if val, ok := cache.Get("a"); !ok || val != 1 {
		t.Errorf("Expected 1, got %v", val)
	}
	time.Sleep(10 * time.Millisecond)
	if val, ok := cache.Get("a"); !ok || val != 1 {
		t.Errorf("Expected 1, got %v", val)
	}

	// 插入第四个值，应该淘汰 "b"
	cache.Set("d", 4)

	if _, ok := cache.Get("b"); ok {
		t.Errorf("Expected 'b' to be evicted")
	}

	// "c" 只访问过一次，应该也被淘汰
	cache.Set("e", 5)
	if _, ok := cache.Get("c"); ok {
		t.Errorf("Expected 'c' to be evicted")
	}

	// "a" 访问过两次，应该还在缓存中
	if val, ok := cache.Get("a"); !ok || val != 1 {
		t.Errorf("Expected 'a' to be in cache")
	}
}
