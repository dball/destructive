package database

import (
	"testing"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestWriteSimple(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	var e, tx ID
	req := Request{
		Claims: []*Claim{
			{E: TempID("1"), A: sys.DbIdent, V: String("test/ident")},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	assert.Positive(t, res.ID)
	assert.Equal(t, map[TempID]ID{TempID("1"): sys.FirstUserID + 1}, res.NewIDs)
	assert.NotNil(t, res.Snapshot)
	e = res.NewIDs[TempID("1")]
	tx = res.ID

	res = db.Write(req)
	assert.NoError(t, res.Error)
	assert.Equal(t, e, res.NewIDs[TempID("1")])
	assert.NotEqual(t, tx, res.ID)
}

func TestWriteAttr(t *testing.T) {
	t.Skip()
	db := NewIndexDatabase(32, 64, 64)
	req := Request{
		Claims: []*Claim{
			{E: TempID("1"), A: sys.DbIdent, V: String("person/name")},
			{E: TempID("1"), A: sys.AttrType, V: sys.AttrTypeString},
			{E: TempID("1"), A: sys.AttrUnique, V: sys.AttrUniqueIdentity},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	assert.Positive(t, res.ID)
	assert.Equal(t, map[TempID]ID{TempID("1"): sys.FirstUserID + 1}, res.NewIDs)
	assert.NotNil(t, res.Snapshot)

	req = Request{
		Claims: []*Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
		},
	}
	res = db.Write(req)
	assert.NoError(t, res.Error)
	assert.Zero(t, res.ID)
	assert.Nil(t, res.NewIDs)
	assert.NotNil(t, res.Snapshot)
}
