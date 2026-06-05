package index

import (
	"slices"
	"testing"
	"time"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

// TestValueTypeRoundTrip exercises every system value type through an EAV index:
// Insert/Find/Select/First/Count/Delete plus Clone independence. It pins that
// values stored in the typed sub-trees (notably bool in the uint tree and inst in
// the int tree) round-trip back to the correct Value, covering the valuer/devaluer
// and reint/reuint paths.
func TestValueTypeRoundTrip(t *testing.T) {
	cases := []struct {
		name     string
		attrType ID
		v1, v2   Value // v1 sorts before v2 in the underlying typed tree
	}{
		{"string", sys.AttrTypeString, String("alpha"), String("beta")},
		{"int", sys.AttrTypeInt, Int(3), Int(7)},
		{"ref", sys.AttrTypeRef, ID(sys.FirstUserID), ID(sys.FirstUserID + 100)},
		{"float", sys.AttrTypeFloat, Float(1.5), Float(2.5)},
		{"bool", sys.AttrTypeBool, Bool(false), Bool(true)},
		{"inst", sys.AttrTypeInst, Inst(time.UnixMilli(1000).UTC()), Inst(time.UnixMilli(2000).UTC())},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			allocate := newAllocator()
			a := allocate()
			idx := NewCompositeIndex(32, EAVIndex, map[ID]ID{a: c.attrType})
			e := allocate()
			tx := allocate()
			d1 := Datum{E: e, A: a, V: c.v1, T: tx}
			d2 := Datum{E: e, A: a, V: c.v2, T: tx}

			assert.False(t, idx.Insert(d1))
			assert.False(t, idx.Insert(d2))
			assert.True(t, idx.Insert(d1), "second insert reports extant")

			assert.True(t, idx.Find(d1))
			assert.True(t, idx.Find(d2))

			// Select(EA) returns both values for the entity-attribute, in tree order,
			// with the V values round-tripped back to their Value type.
			got := slices.Collect(idx.Select(EA, Datum{E: e, A: a}))
			assert.Equal(t, []Datum{d1, d2}, got)

			assert.Equal(t, 2, idx.Count(EA, Datum{E: e, A: a}))

			first, ok := idx.First(EA, Datum{E: e, A: a})
			assert.True(t, ok)
			assert.Equal(t, d1, first)

			// Clone is independent: deleting from the original leaves the clone intact.
			clone := idx.Clone()
			assert.True(t, idx.Delete(d1))
			assert.False(t, idx.Find(d1))
			assert.True(t, clone.Find(d1))
			assert.Equal(t, 1, idx.Count(EA, Datum{E: e, A: a}))
			assert.Equal(t, 2, clone.Count(EA, Datum{E: e, A: a}))
		})
	}
}

// TestAEVSelectByAttribute covers the AEV index and the A partial — selecting all
// datums for an attribute across entities — which the existing suite never exercises
// (it only shows A is unsatisfiable on an EAV index).
func TestAEVSelectByAttribute(t *testing.T) {
	allocate := newAllocator()
	a := allocate()
	idx := NewCompositeIndex(32, AEVIndex, map[ID]ID{a: sys.AttrTypeInt})
	tx := allocate()
	e1 := allocate()
	e2 := allocate()
	e3 := allocate()
	idx.Insert(Datum{E: e2, A: a, V: Int(20), T: tx})
	idx.Insert(Datum{E: e1, A: a, V: Int(10), T: tx})
	idx.Insert(Datum{E: e3, A: a, V: Int(30), T: tx})

	// AEV orders by attribute, then entity, then value.
	expected := []Datum{
		{E: e1, A: a, V: Int(10), T: tx},
		{E: e2, A: a, V: Int(20), T: tx},
		{E: e3, A: a, V: Int(30), T: tx},
	}
	assert.Equal(t, expected, slices.Collect(idx.Select(A, Datum{A: a})))
	assert.Equal(t, 3, idx.Count(A, Datum{A: a}))
	first, ok := idx.First(A, Datum{A: a})
	assert.True(t, ok)
	assert.Equal(t, expected[0], first)
}

// TestAVECountAndFirst covers the AVE index Count alongside the already-tested AV
// First, using a value-typed attribute.
func TestAVECountAndFirst(t *testing.T) {
	allocate := newAllocator()
	a := allocate()
	idx := NewCompositeIndex(32, AVEIndex, map[ID]ID{a: sys.AttrTypeString})
	tx := allocate()
	e1 := allocate()
	e2 := allocate()
	idx.Insert(Datum{E: e1, A: a, V: String("ident/one"), T: tx})
	idx.Insert(Datum{E: e2, A: a, V: String("ident/two"), T: tx})

	assert.Equal(t, 1, idx.Count(AV, Datum{A: a, V: String("ident/two")}))
	assert.Equal(t, 0, idx.Count(AV, Datum{A: a, V: String("ident/missing")}))
	first, ok := idx.First(AV, Datum{A: a, V: String("ident/one")})
	assert.True(t, ok)
	assert.Equal(t, Datum{E: e1, A: a, V: String("ident/one"), T: tx}, first)
}

// TestVAEInsertFindDelete covers the VAE (back-reference) index. The composite
// index does not wire a Select partial for vae today, so this asserts only the
// membership operations the database actually uses on it.
func TestVAEInsertFindDelete(t *testing.T) {
	allocate := newAllocator()
	a := allocate()
	idx := NewCompositeIndex(32, VAEIndex, map[ID]ID{a: sys.AttrTypeRef})
	tx := allocate()
	e := allocate()
	ref := allocate()
	d := Datum{E: e, A: a, V: ID(ref), T: tx}

	assert.False(t, idx.Insert(d))
	assert.True(t, idx.Find(d))
	assert.True(t, idx.Delete(d))
	assert.False(t, idx.Find(d))
	assert.False(t, idx.Delete(d))
}

// TestFirstNegativeInt covers a fixed bug: First on an int- or inst-typed attribute
// used to start its scan from the zero value instead of math.MinInt64, skipping
// negative values. First(EA) now returns the true minimum, -5.
func TestFirstNegativeInt(t *testing.T) {
	allocate := newAllocator()
	a := allocate()
	idx := NewCompositeIndex(32, EAVIndex, map[ID]ID{a: sys.AttrTypeInt})
	e := allocate()
	tx := allocate()
	idx.Insert(Datum{E: e, A: a, V: Int(-5), T: tx})
	idx.Insert(Datum{E: e, A: a, V: Int(4), T: tx})

	assert.Equal(t, []Datum{
		{E: e, A: a, V: Int(-5), T: tx},
		{E: e, A: a, V: Int(4), T: tx},
	}, slices.Collect(idx.Select(EA, Datum{E: e, A: a})))
	assert.Equal(t, 2, idx.Count(EA, Datum{E: e, A: a}))

	first, ok := idx.First(EA, Datum{E: e, A: a})
	assert.True(t, ok)
	assert.Equal(t, Datum{E: e, A: a, V: Int(-5), T: tx}, first)
}

// TestAllDatums asserts All() yields every datum across the typed sub-trees in
// global eav order (by entity, then attribute), recovering bool and inst values.
func TestAllDatums(t *testing.T) {
	allocate := newAllocator()
	aName := allocate() // string
	aAge := allocate()  // int
	aOk := allocate()   // bool (uint tree)
	idx := NewCompositeIndex(32, EAVIndex, map[ID]ID{
		aName: sys.AttrTypeString,
		aAge:  sys.AttrTypeInt,
		aOk:   sys.AttrTypeBool,
	})
	tx := allocate()
	e1 := allocate()
	e2 := allocate()
	d1Name := Datum{E: e1, A: aName, V: String("e1"), T: tx}
	d1Ok := Datum{E: e1, A: aOk, V: Bool(true), T: tx}
	d2Age := Datum{E: e2, A: aAge, V: Int(2), T: tx}
	for _, d := range []Datum{d2Age, d1Ok, d1Name} {
		idx.Insert(d)
	}

	expected := []Datum{d1Name, d1Ok, d2Age} // e1 (aName<aOk) then e2
	assert.Equal(t, expected, slices.Collect(idx.All()))
}

// TestSelectEGlobalOrdering asserts that Select(E) returns an entity's datums in
// global attribute order, merged across the typed sub-trees, rather than grouped by
// storage type.
func TestSelectEGlobalOrdering(t *testing.T) {
	allocate := newAllocator()
	aStr := allocate()   // string tree
	aInt := allocate()   // int tree
	aRef := allocate()   // uint tree
	aFloat := allocate() // float tree
	aBool := allocate()  // uint tree
	aInst := allocate()  // int tree
	idx := NewCompositeIndex(32, EAVIndex, map[ID]ID{
		aStr:   sys.AttrTypeString,
		aInt:   sys.AttrTypeInt,
		aRef:   sys.AttrTypeRef,
		aFloat: sys.AttrTypeFloat,
		aBool:  sys.AttrTypeBool,
		aInst:  sys.AttrTypeInst,
	})
	e := allocate()
	tx := allocate()
	dStr := Datum{E: e, A: aStr, V: String("x"), T: tx}
	dInt := Datum{E: e, A: aInt, V: Int(1), T: tx}
	dRef := Datum{E: e, A: aRef, V: ID(sys.FirstUserID), T: tx}
	dFloat := Datum{E: e, A: aFloat, V: Float(1.5), T: tx}
	dBool := Datum{E: e, A: aBool, V: Bool(true), T: tx}
	dInst := Datum{E: e, A: aInst, V: Inst(time.UnixMilli(1000).UTC()), T: tx}
	for _, d := range []Datum{dStr, dInt, dRef, dFloat, dBool, dInst} {
		idx.Insert(d)
	}

	// Datums come back in attribute-id order, regardless of which sub-tree stores them.
	expected := []Datum{dStr, dInt, dRef, dFloat, dBool, dInst}
	assert.Equal(t, expected, slices.Collect(idx.Select(E, Datum{E: e})))

	// First(E) returns the lowest-attribute datum, correctly typed.
	first, ok := idx.First(E, Datum{E: e})
	assert.True(t, ok)
	assert.Equal(t, dStr, first)
}
