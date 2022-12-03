// Package index provides for datum indexes implemented on btrees.
package index

import (
	. "github.com/dball/destructive/internal/types"
	"golang.org/x/exp/constraints"

	"github.com/google/btree"
)

// TypedDatum represents a datum with a specific V type. These will use less memory than interface V types
// and their values can be compared with an operator.
type TypedDatum[X constraints.Ordered] struct {
	E ID
	A ID
	V X
	T ID
}

// Index instances maintain sorted sets of typed datums. Indexes are safe for concurrent read
// operations but may not be safe for concurrent write operations, including cloning.
type Index[X constraints.Ordered] interface {
	// Find returns true if the given datum is in the index.
	Find(datum TypedDatum[X]) (extant bool)
	// Insert ensures the given datum is present in the index, returning true if it was already.
	Insert(datum TypedDatum[X]) (extant bool)
	// Delete ensures the given datum is not present in the index, returning true if it was.
	Delete(datum TypedDatum[X]) (extant bool)
	// Clone returns a copy of the index. Both the original and the clone may be changed hereafter
	// without either affecting the other.
	Clone() (clone Index[X])
}

type btreeIndex[X constraints.Ordered] struct {
	// TODO the struct isn't necessary or even desirable unless we have more things to say about
	// our trees, but I could not express this as a generically typed type alias.
	tree *btree.BTreeG[TypedDatum[X]]
}

// NewBTreeIndex returns a btree index of the given degree that sorts its set of typed datums
// according to the given lesser function, which returns true iff the first arg is less than
// the second.
func NewBTreeIndex[X constraints.Ordered](degree int, lesser Lesser[X]) (index Index[X]) {
	index = &btreeIndex[X]{tree: btree.NewG(degree, btree.LessFunc[TypedDatum[X]](lesser))}
	return
}

func (index *btreeIndex[X]) Find(datum TypedDatum[X]) (found bool) {
	found = index.tree.Has(datum)
	return
}

func (index *btreeIndex[X]) Insert(datum TypedDatum[X]) (extant bool) {
	extant = index.Find(datum)
	if !extant {
		// We would use this directly for efficiency, but this overwrites an extant value,
		// while we choose to retain it, preferring the earliest T value introducing a datum.
		index.tree.ReplaceOrInsert(datum)
	}
	return
}

func (index *btreeIndex[X]) Delete(datum TypedDatum[X]) (extant bool) {
	_, extant = index.tree.Delete(datum)
	return
}

func (index *btreeIndex[X]) Clone() (clone Index[X]) {
	return &btreeIndex[X]{tree: index.tree.Clone()}
}
