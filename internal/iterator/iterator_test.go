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

type watch struct {
	total    int64
	realized int
	closed   bool
}

func (w *watch) Each(accept Accept[int64]) {
	for i := 0; int64(i) < w.total; i++ {
		if !accept(int64(i)) {
			w.closed = true
			return
		}
		w.realized++
	}
	w.closed = true
}

func TestLaziness(t *testing.T) {
	w := watch{total: 5}
	iter := BuildIterator[int64](&w)
	assert.True(t, iter.Next())
	assert.True(t, iter.Next())
	assert.True(t, iter.Next())
	assert.Equal(t, 3, w.realized)
	iter.Stop()
	// the watch is not registered closed immediately because the iterator produces
	// asynchronously.
	assert.False(t, iter.Next())
	assert.True(t, w.closed)
	assert.False(t, iter.Next())
}

func TestIterators(t *testing.T) {
	iter := Iterators(RangeBetween(1, 5), RangeBetween(2, 6))
	x := iter.Drain()
	assert.Equal(t, x, []int64{1, 2, 3, 4, 2, 3, 4, 5})
}

func TestIteratorsEarlyClose(t *testing.T) {
	w := watch{total: 5}
	i1 := RangeBetween(1, 5)
	i2 := BuildIterator[int64](&w)
	iter := Iterators(i1, i2)
	assert.True(t, iter.Next())
	iter.Stop()
	assert.False(t, iter.Next())
	// the watch is not registered closed immediately here because the second
	// iterator may not have read its stop channel yet. calling Next() on it
	// directly realizes the closedness for test.
	assert.False(t, i2.Next())
	assert.True(t, w.closed)
}
