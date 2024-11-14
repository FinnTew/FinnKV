package algo

import (
	"FinnKV/internal/algo"
	"strconv"
	"sync"
	"testing"
)

func TestSkipList(t *testing.T) {
	lessFunc := func(a, b int) bool { return a < b }
	skiplist := algo.NewSkipList[int, string](lessFunc)

	skiplist.Add(1, "one")
	skiplist.Add(2, "two")
	skiplist.Add(3, "three")

	if val, found := skiplist.Find(2); !found || val != "two" {
		t.Errorf("expected to find 2 with value 'two', got %v", val)
	}

	if skiplist.Len() != 3 {
		t.Errorf("expected length 3, got %d", skiplist.Len())
	}

	skiplist.Del(2)

	if _, found := skiplist.Find(2); found {
		t.Error("expected 2 to be deleted")
	}

	if skiplist.Len() != 2 {
		t.Errorf("expected length 2, got %d", skiplist.Len())
	}

	iter := skiplist.Iterator()
	var keys []int
	for {
		key, _, ok := iter()
		if !ok {
			break
		}
		keys = append(keys, key)
	}

	if len(keys) != 2 || keys[0] != 1 || keys[1] != 3 {
		t.Errorf("unexpected keys in iterator: %v", keys)
	}
}

func benchmarkSkipListConcurrentAdd(sl *algo.SkipList[int, string], b *testing.B, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < b.N; i++ {
		sl.Add(i, "value"+strconv.Itoa(i))
	}
}

func BenchmarkSkipListConcurrent(b *testing.B) {
	lessFunc := func(a, b int) bool { return a < b }
	skiplist := algo.NewSkipList[int, string](lessFunc)
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go benchmarkSkipListConcurrentAdd(skiplist, b, &wg)
	}
	wg.Wait()
}
