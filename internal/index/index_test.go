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

	assert.Equal(t, []Datum{d1}, idx.Select(EA, d2).Drain())
	assert.True(t, idx.Find(d2))
	assert.True(t, idx.Delete(d2))
	assert.False(t, idx.Delete(d2))
	assert.False(t, idx.Find(d2))
}
