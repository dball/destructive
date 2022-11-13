// Package iterator provides forwards-only iterators over enumerable, potentially infinite collections, allowing for early termination.
package iterator

type void struct{}

// Accept is a predicate that receives a value from an iterator
// and returns true if more values are desired.
type Accept[T any] func(T) bool

// Collection is a source for iterable values.
type Collection[T any] interface {
	Each(Accept[T])
}

// Iterator is a lazy, forwards-only iterator over an iterable collection with early termination.
type Iterator[T any] struct {
	stop    chan void
	values  chan T
	current T
}

// BuildIterator returns a reference to an iterator for the given collection.
func BuildIterator[T any](coll Collection[T]) *Iterator[T] {
	values := make(chan T)
	stop := make(chan void)
	go func() {
		defer close(values)
		coll.Each(func(value T) bool {
			select {
			case values <- value:
				return true
			case <-stop:
				return false
			}
		})
	}()
	return &Iterator[T]{stop: stop, values: values}
}

// Next advances the iterator, returning true if successful.
func (iter *Iterator[T]) Next() (ok bool) {
	iter.current, ok = <-iter.values
	return
}

// Value returns the value of the iterable collection at the current position of the iterator.
func (iter *Iterator[T]) Value() T {
	return iter.current
}

// Stop invalidates the iterator, useful for partial iteration over lazy sequences.
func (iter *Iterator[T]) Stop() {
	close(iter.stop)
}

// Drain returns a slice of the values remaining in the iterator.
//
// This is not advisable on infinite sequences.
func (iter *Iterator[T]) Drain() []T {
	values := []T{}
	for iter.Next() {
		values = append(values, iter.Value())
	}
	return values
}

// Reduce fully reduces the iterated collection by adding the values sequentially to the given init value.
func Reduce[T any, U any](iter *Iterator[T], add func(U, T) U, init U) U {
	result := init
	for iter.Next() {
		result = add(result, iter.Value())
	}
	return result
}

// Iterators is a collection of iterators that will be iterated consecutively.
type Iterators[T any] []*Iterator[T]

func (iters Iterators[T]) Each(accept Accept[T]) {
	for i, iter := range iters {
		for iter.Next() {
			if !accept(iter.Value()) {
				for j := i + 1; j < len(iters); j++ {
					iters[j].Stop()
				}
				return
			}
		}
	}
}

// Slice is a wrapper type for slices.
type Slice[T any] []T

func (slice Slice[T]) Each(accept Accept[T]) {
	for _, value := range slice {
		if !accept(value) {
			return
		}
	}
}

// Note if this became more than just example code, we'd want a ranger
// for each common collection to reduce space, e.g. assuming step 1.

type ranger struct {
	start   int64
	stop    int64
	step    int64
	hasStop bool
}

func (r *ranger) Each(accept Accept[int64]) {
	for i := r.start; !r.hasStop || i < r.stop; i += r.step {
		if !accept(i) {
			return
		}
	}
}

func RangeAll() *Iterator[int64] {
	return BuildIterator[int64](&ranger{step: 1})
}

func RangeBetween(start int64, stop int64) *Iterator[int64] {
	return BuildIterator[int64](&ranger{start: start, stop: stop, hasStop: true, step: 1})
}
