package index

import (
	"cmp"
	"iter"

	. "github.com/dball/destructive/internal/types"
)

// CompareAll treats every datum as equal, so a Select driven by it yields the
// entire tree (in its native sort order) starting from the zero datum.
func CompareAll[X cmp.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	return 0
}

// lessEA reports whether datum a precedes datum b in entity-then-attribute order.
// It deliberately ignores V: an attribute has a single value type, so all datums
// for a given (E, A) live in the same typed sub-tree and never need to be merged
// against another tree on V.
func lessEA(a Datum, b Datum) bool {
	if a.E != b.E {
		return a.E < b.E
	}
	return a.A < b.A
}

// mergeEAV merges the already-eav-sorted typed sub-sequences into a single globally
// eav-ordered sequence. Because each (E, A) pair belongs to exactly one sub-sequence,
// merging on (E, A) alone preserves the within-sequence V ordering.
func mergeEAV(seqs ...iter.Seq[Datum]) iter.Seq[Datum] {
	return func(yield func(Datum) bool) {
		n := len(seqs)
		nexts := make([]func() (Datum, bool), n)
		heads := make([]Datum, n)
		have := make([]bool, n)
		for i, seq := range seqs {
			next, stop := iter.Pull(seq)
			defer stop()
			nexts[i] = next
			heads[i], have[i] = next()
		}
		for {
			best := -1
			for i := 0; i < n; i++ {
				if have[i] && (best == -1 || lessEA(heads[i], heads[best])) {
					best = i
				}
			}
			if best == -1 {
				return
			}
			if !yield(heads[best]) {
				return
			}
			heads[best], have[best] = nexts[best]()
		}
	}
}

// All returns every datum in the composite index in global eav order. The int and
// uint sub-trees are mapped back through reint/reuint to recover the inst and bool
// values stored in them.
func (idx *CompositeIndex) All() iter.Seq[Datum] {
	strings := idx.strings.Select(CompareAll[string], stringValuer.valuer, TypedDatum[string]{})
	ints := idx.ints.Select(CompareAll[int64], intValuer.valuer, TypedDatum[int64]{})
	uints := idx.uints.Select(CompareAll[uint64], refValuer.valuer, TypedDatum[uint64]{})
	floats := idx.floats.Select(CompareAll[float64], floatValuer.valuer, TypedDatum[float64]{})
	return mergeEAV(strings, mapSeq(ints, idx.reint), mapSeq(uints, idx.reuint), floats)
}
