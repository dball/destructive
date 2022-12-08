package index

import (
	"testing"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func newAllocator() func() ID {
	nextID := sys.FirstUserID
	return func() (id ID) {
		id = nextID
		nextID++
		return
	}
}

func TestIndex(t *testing.T) {
	allocate := newAllocator()
	a := allocate()
	idx := NewCompositeIndex(32, EAVIndex, map[ID]ID{a: sys.AttrTypeString})
	e := allocate()
	tx := allocate()
	d1 := Datum{E: e, A: a, V: String("donald"), T: tx}
	d2 := Datum{E: e, A: a, V: String("Donald"), T: tx}

	assert.False(t, idx.Insert(d1))
	assert.True(t, idx.Insert(d1))
	assert.True(t, idx.Find(d1))
	assert.False(t, idx.Find(d2))

	assert.False(t, idx.Insert(d2))
	assert.True(t, idx.Insert(d2))

	assert.True(t, idx.Find(d1))
	assert.True(t, idx.Find(d2))

	assert.Equal(t, []Datum{d2, d1}, idx.Select(EA, Datum{E: e, A: a}).Drain())
	assert.Equal(t, []Datum{}, idx.Select(EA, Datum{E: e - 1, A: a}).Drain())
	assert.Equal(t, []Datum{}, idx.Select(EA, Datum{E: e + 1, A: a}).Drain())

	assert.True(t, idx.Delete(d1))
	assert.False(t, idx.Find(d1))

	clone := idx.Clone()
	assert.True(t, clone.Find(d2))
	assert.True(t, idx.Delete(d2))
	assert.False(t, idx.Find(d2))
	assert.True(t, clone.Find(d2))
}

func TestIndexTimeProperties(t *testing.T) {
	allocate := newAllocator()
	a := allocate()
	idx := NewCompositeIndex(32, EAVIndex, map[ID]ID{a: sys.AttrTypeString})
	e := allocate()
	t1 := allocate()
	t2 := allocate()
	d1 := Datum{E: e, A: a, V: String("Donald"), T: t1}
	d2 := Datum{E: e, A: a, V: String("Donald"), T: t2}

	assert.False(t, idx.Insert(d1))

	assert.Equal(t, []Datum{d1}, idx.Select(E, d2).Drain())
	assert.True(t, idx.Find(d2))
	assert.True(t, idx.Delete(d2))
	assert.False(t, idx.Delete(d2))
	assert.False(t, idx.Find(d2))
}

func TestIndexSelection(t *testing.T) {
	allocate := newAllocator()
	a1 := allocate()
	a2 := allocate()
	a3 := allocate()
	idx := NewCompositeIndex(32, EAVIndex, map[ID]ID{
		a1: sys.AttrTypeInt,
		a2: sys.AttrTypeInt,
		a3: sys.AttrTypeInt,
	})
	tx := allocate()
	n := 3
	var es []ID
	for i := 0; i < n; i++ {
		e := allocate()
		es = append(es, e)
		m := 5
		for j := 0; j < 5; j++ {
			idx.Insert(Datum{E: e, A: a1, V: Int(j), T: tx})
			idx.Insert(Datum{E: e, A: a2, V: Int(j + 1), T: tx})
			// We insert in reverse order, and the index will sort on select
			idx.Insert(Datum{E: e, A: a3, V: Int(m - j), T: tx})
		}
	}

	t.Run("ea1", func(t *testing.T) {
		expected := []Datum{
			{E: es[0], A: a1, V: Int(0), T: tx},
			{E: es[0], A: a1, V: Int(1), T: tx},
			{E: es[0], A: a1, V: Int(2), T: tx},
			{E: es[0], A: a1, V: Int(3), T: tx},
			{E: es[0], A: a1, V: Int(4), T: tx},
		}
		assert.Equal(t, expected, idx.Select(EA, Datum{E: es[0], A: a1}).Drain())
	})

	t.Run("e", func(t *testing.T) {
		expected := []Datum{
			{E: es[1], A: a1, V: Int(0), T: tx},
			{E: es[1], A: a1, V: Int(1), T: tx},
			{E: es[1], A: a1, V: Int(2), T: tx},
			{E: es[1], A: a1, V: Int(3), T: tx},
			{E: es[1], A: a1, V: Int(4), T: tx},
			{E: es[1], A: a2, V: Int(1), T: tx},
			{E: es[1], A: a2, V: Int(2), T: tx},
			{E: es[1], A: a2, V: Int(3), T: tx},
			{E: es[1], A: a2, V: Int(4), T: tx},
			{E: es[1], A: a2, V: Int(5), T: tx},
			{E: es[1], A: a3, V: Int(1), T: tx},
			{E: es[1], A: a3, V: Int(2), T: tx},
			{E: es[1], A: a3, V: Int(3), T: tx},
			{E: es[1], A: a3, V: Int(4), T: tx},
			{E: es[1], A: a3, V: Int(5), T: tx},
		}
		assert.Equal(t, expected, idx.Select(E, Datum{E: es[1]}).Drain())
	})

	t.Run("ae", func(t *testing.T) {
		expected := []Datum{
			{E: es[2], A: a3, V: Int(1), T: tx},
			{E: es[2], A: a3, V: Int(2), T: tx},
			{E: es[2], A: a3, V: Int(3), T: tx},
			{E: es[2], A: a3, V: Int(4), T: tx},
			{E: es[2], A: a3, V: Int(5), T: tx},
		}
		assert.Equal(t, expected, idx.Select(AE, Datum{E: es[2], A: a3}).Drain())
	})

	// TODO this is unsatisfiable by the eav index. Currently, we return an empty
	// iterator, but that's as accidental as anything. What should we return?
	t.Run("a", func(t *testing.T) {
		expected := []Datum{}
		assert.Equal(t, expected, idx.Select(A, Datum{A: a2}).Drain())
	})
}
