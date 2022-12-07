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

type Wrapper[X constraints.Ordered] func(datum Datum) (typed TypedDatum[X])

type Unwrapper[X constraints.Ordered] func(typed TypedDatum[X]) (datum Datum)

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
	Select(comparer Comparer[X], unwrap Unwrapper[X], datum TypedDatum[X]) (iter *iterator.Iterator[Datum])
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
	unwrap   Unwrapper[X]
}

func (sel *selection[X]) Each(accept iterator.Accept[Datum]) {
	sel.idx.tree.AscendGreaterOrEqual(sel.datum, func(datum TypedDatum[X]) bool {
		switch sel.comparer(sel.datum, datum) {
		case 0:
			return accept(sel.unwrap(datum))
		case 1, -1:
			return false
		default:
			panic("index.typed.selection.each")
		}
	})
}

func (idx *btreeIndex[X]) Select(comparer Comparer[X], unwrap Unwrapper[X], datum TypedDatum[X]) (iter *iterator.Iterator[Datum]) {
	return iterator.BuildIterator[Datum](&selection[X]{idx, comparer, datum, unwrap})
}
