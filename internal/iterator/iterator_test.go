package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceIterator(t *testing.T) {
	x := []int{3, 2, 1}
	slice := Slice[int](x)
	iter := BuildIterator[int](slice)
	assert.True(t, iter.Next())
	assert.Equal(t, 3, iter.Value())
	assert.True(t, iter.Next())
	assert.Equal(t, 2, iter.Value())
	assert.True(t, iter.Next())
	assert.Equal(t, 1, iter.Value())
	assert.False(t, iter.Next())
}

func TestRangeAll(t *testing.T) {
	x := []int64{}
	r := RangeAll()
	for i := 0; i < 10; i++ {
		assert.True(t, r.Next())
		x = append(x, r.Value())
	}
	assert.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, x)
}

func TestRangeBetween(t *testing.T) {
	x := RangeBetween(10, 13).Drain()
	assert.Equal(t, []int64{10, 11, 12}, x)
}

func TestReduce(t *testing.T) {
	x := RangeBetween(0, 5)
	sum := Reduce(x, func(total int64, n int64) int64 { return total + n }, 0)
	assert.Equal(t, int64(10), sum)
}
