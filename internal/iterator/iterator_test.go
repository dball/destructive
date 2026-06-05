package iterator

import (
	"iter"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConcat(t *testing.T) {
	seq := Concat(slices.Values([]int{1, 2}), slices.Values([]int{3, 4, 5}))
	assert.Equal(t, []int{1, 2, 3, 4, 5}, slices.Collect(seq))
}

func TestConcatEmpty(t *testing.T) {
	assert.Empty(t, slices.Collect(Concat[int]()))
}

func TestConcatMixedEmpty(t *testing.T) {
	// Empty sub-sequences interspersed with non-empty ones contribute nothing
	// but must not interrupt the flow of the others.
	seq := Concat(
		slices.Values([]int{}),
		slices.Values([]int{1, 2}),
		slices.Values([]int{}),
		slices.Values([]int{3}),
		slices.Values([]int{}),
	)
	assert.Equal(t, []int{1, 2, 3}, slices.Collect(seq))
}

func TestConcatEarlyTermination(t *testing.T) {
	// touched records how many values each sub-sequence yielded, so we can prove the
	// composite does no work past the point the consumer stops.
	var touched [2]int
	track := func(i int, vals ...int) iter.Seq[int] {
		return func(yield func(int) bool) {
			for _, v := range vals {
				touched[i]++
				if !yield(v) {
					return
				}
			}
		}
	}
	seq := Concat(track(0, 1, 2, 3), track(1, 4, 5, 6))
	var got []int
	for v := range seq {
		got = append(got, v)
		if len(got) == 2 {
			break
		}
	}
	assert.Equal(t, []int{1, 2}, got)
	// The first sub-sequence yielded only up to the breakpoint; the second was never started.
	assert.Equal(t, 2, touched[0])
	assert.Equal(t, 0, touched[1])
}
