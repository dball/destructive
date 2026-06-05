package sys

import (
	"testing"
	"time"

	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestValidValue(t *testing.T) {
	cases := []struct {
		name  string
		typ   ID
		value Value
		ok    bool
	}{
		{"ref ok", AttrTypeRef, ID(7), true},
		{"ref wrong", AttrTypeRef, String("x"), false},
		{"string ok", AttrTypeString, String("x"), true},
		{"string wrong", AttrTypeString, Int(1), false},
		{"int ok", AttrTypeInt, Int(1), true},
		{"int wrong", AttrTypeInt, Float(1), false},
		{"bool ok", AttrTypeBool, Bool(true), true},
		{"bool wrong", AttrTypeBool, Int(1), false},
		{"inst ok", AttrTypeInst, Inst(time.UnixMilli(0).UTC()), true},
		{"inst wrong", AttrTypeInst, Int(0), false},
		{"float ok", AttrTypeFloat, Float(1.5), true},
		{"float wrong", AttrTypeFloat, Int(1), false},
		{"unknown type", ID(0), String("x"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.ok, ValidValue(c.typ, c.value))
		})
	}
}

func TestValidUnique(t *testing.T) {
	assert.True(t, ValidUnique(AttrUniqueIdentity))
	assert.True(t, ValidUnique(AttrUniqueValue))
	assert.False(t, ValidUnique(AttrType))
	assert.False(t, ValidUnique(ID(0)))
}

func TestValidAttrType(t *testing.T) {
	for _, id := range []ID{AttrTypeRef, AttrTypeString, AttrTypeInt, AttrTypeBool, AttrTypeInst, AttrTypeFloat} {
		assert.True(t, ValidAttrType(id), "expected %v to be a valid attr type", id)
	}
	assert.False(t, ValidAttrType(AttrUniqueIdentity))
	assert.False(t, ValidAttrType(ID(0)))
}

func TestValidAttrCardinality(t *testing.T) {
	assert.True(t, ValidAttrCardinality(AttrCardinalityOne))
	assert.True(t, ValidAttrCardinality(AttrCardinalityMany))
	assert.False(t, ValidAttrCardinality(AttrType))
	assert.False(t, ValidAttrCardinality(ID(0)))
}

func TestValidAttrRefType(t *testing.T) {
	assert.True(t, ValidAttrRefType(AttrRefTypeDependent))
	assert.False(t, ValidAttrRefType(AttrTypeRef))
	assert.False(t, ValidAttrRefType(ID(0)))
}

func TestValidUserIdent(t *testing.T) {
	assert.True(t, ValidUserIdent(String("person/name")))
	assert.True(t, ValidUserIdent(String("syscall/foo")))
	assert.False(t, ValidUserIdent(String("sys/db/ident")))
	assert.False(t, ValidUserIdent(String("sys/")))
}
