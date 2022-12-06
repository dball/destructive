package index

import (
	"testing"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	id := sys.FirstUserID
	a := id
	id++
	idx := NewCompositeIndex(32, EAVIndex, map[ID]ID{a: sys.AttrTypeString})
	e := id
	id++
	d1 := Datum{E: e, A: a, V: String("donald"), T: e}
	d2 := Datum{E: e, A: a, V: String("Donald"), T: e}

	assert.False(t, idx.Insert(d1))
	assert.True(t, idx.Insert(d1))
	assert.True(t, idx.Find(d1))
	assert.False(t, idx.Find(d2))

	assert.False(t, idx.Insert(d2))
	assert.True(t, idx.Insert(d2))

	assert.True(t, idx.Find(d1))
	assert.True(t, idx.Find(d2))

	assert.True(t, idx.Delete(d1))
	assert.False(t, idx.Find(d1))

	clone := idx.Clone()
	assert.True(t, clone.Find(d2))
	assert.True(t, idx.Delete(d2))
	assert.False(t, idx.Find(d2))
	assert.True(t, clone.Find(d2))
}
