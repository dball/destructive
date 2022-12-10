package index

import (
	"github.com/dball/destructive/internal/iterator"
	. "github.com/dball/destructive/internal/types"
	"golang.org/x/exp/constraints"

	"github.com/google/btree"
)

// TypedDatum represents a datum with a specific V type. These will use less memory than interface V types
// and their values can be compared with an operator. Note that we probably lose throughput and add memory
// churn on select as we convert TypedDatum instances into Datum instances. Possibly it would be least bad
// to store the data as datums with interface v and use unsafe pointer foo to interpret the v memory
// efficiently based on the type of a.
type TypedDatum[X constraints.Ordered] struct {
	E ID
	A ID
	V X
	T ID
}

type Valuer[X constraints.Ordered] func(v X) (value Value)
type Devaluer[X constraints.Ordered] func(value Value) (v X)
type TypeValuer[X constraints.Ordered] struct {
	valuer   Valuer[X]
	devaluer Devaluer[X]
}

// TypedIndex instances maintain sorted sets of typed datums. Indexes are safe for concurrent read
// operations but may not be safe for concurrent write operations, including cloning.
type TypedIndex[X constraints.Ordered] interface {
	// Find returns true if the given datum is in the index.
	Find(datum TypedDatum[X]) (extant bool)
	// Insert ensures the given datum is present in the index, returning true if it was already.
	Insert(datum TypedDatum[X]) (extant bool)
	// Delete ensures the given datum is not present in the index, returning true if it was.
	Delete(datum TypedDatum[X]) (extant bool)
	// Clone returns a copy of the index. Both the original and the clone may be changed hereafter
	// without either affecting the other.
	Clone() (clone TypedIndex[X])
	// Select returns an ascending iterator of datums starting at the point datum would occupy in the
	// ordered set for which the comparer returns 0. The v values of those datums are converted from
	// indexed storage values to datum Values with the valuer function.
	Select(comparer Comparer[X], valuer Valuer[X], datum TypedDatum[X]) (iter *iterator.Iterator[Datum])
	// First returns the first datum after the point datum would occupy in the ordered set for which
	// the comparer returns 0, if any. The v value of the datum is converted from indexed storage
	// value to datum Value with the valuer function.
	First(comparer Comparer[X], valuer Valuer[X], datum TypedDatum[X]) (match Datum, extant bool)
}

type btreeIndex[X constraints.Ordered] struct {
	// TODO the struct isn't necessary or even desirable unless we have more things to say about
	// our trees, but I could not express this as a generically typed type alias.
	tree *btree.BTreeG[TypedDatum[X]]
}

// NewBTreeIndex returns a btree index of the given degree that sorts its set of typed datums
// according to the given lesser function, which returns true iff the first arg is less than
// the second.
func NewBTreeIndex[X constraints.Ordered](degree int, lesser Lesser[X]) (index TypedIndex[X]) {
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

func (index *btreeIndex[X]) Clone() (clone TypedIndex[X]) {
	return &btreeIndex[X]{tree: index.tree.Clone()}
}

type selection[X constraints.Ordered] struct {
	idx      *btreeIndex[X]
	comparer Comparer[X]
	datum    TypedDatum[X]
	valuer   Valuer[X]
}

func (sel *selection[X]) Each(accept iterator.Accept[Datum]) {
	sel.idx.tree.AscendGreaterOrEqual(sel.datum, func(datum TypedDatum[X]) bool {
		switch sel.comparer(sel.datum, datum) {
		case 0:
			return accept(Datum{E: datum.E, A: datum.A, V: sel.valuer(datum.V), T: datum.T})
		case 1, -1:
			return false
		default:
			panic("index.typed.selection.each")
		}
	})
}

func (idx *btreeIndex[X]) Select(comparer Comparer[X], valuer Valuer[X], datum TypedDatum[X]) (iter *iterator.Iterator[Datum]) {
	return iterator.BuildIterator[Datum](&selection[X]{idx, comparer, datum, valuer})
}

func (idx *btreeIndex[X]) First(comparer Comparer[X], valuer Valuer[X], datum TypedDatum[X]) (match Datum, extant bool) {
	idx.tree.AscendGreaterOrEqual(datum, func(d TypedDatum[X]) bool {
		if comparer(datum, d) == 0 {
			match = Datum{E: d.E, A: d.A, V: valuer(d.V), T: d.T}
			extant = true
		}
		return false
	})
	return
}
