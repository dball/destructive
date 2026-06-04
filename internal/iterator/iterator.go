// Package iterator provides small helpers over the standard library iter package
// for sequences produced lazily with early termination. Producers should expose
// iter.Seq directly; these helpers cover the few operations the standard library
// does not provide.
package iterator

import "iter"

// Concat returns a sequence that yields the values of each given sequence in turn.
// If a consumer stops early, the sequence currently being read is abandoned and
// the remaining sequences are never started.
func Concat[T any](seqs ...iter.Seq[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, seq := range seqs {
			for v := range seq {
				if !yield(v) {
					return
				}
			}
		}
	}
}
