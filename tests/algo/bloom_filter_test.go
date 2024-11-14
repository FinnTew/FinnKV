package algo

import (
	"FinnKV/internal/algo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBloomFilter(t *testing.T) {
	bf := algo.NewBloomFilter(10000, 0.01)

	input := [][]byte{
		[]byte("aaaa"),
		[]byte("bbbb"),
		[]byte("cccc"),
		[]byte("dddd"),
	}

	for _, data := range input {
		bf.Add(data)
	}

	for _, data := range input {
		assert.True(t, bf.Contains(data))
	}

	assert.False(t, bf.Contains([]byte("eeee")))
}
