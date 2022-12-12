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

	match, ok := res.Snapshot.Find(Claim{E: e, A: sys.DbIdent, V: String("test/ident")})
	assert.True(t, ok)
	assert.Equal(t, Datum{E: e, A: sys.DbIdent, V: String("test/ident")}, match)
	snapshot := db.Read()
	match, ok = snapshot.Find(Claim{E: e, A: sys.DbIdent, V: String("test/ident")})
	assert.True(t, ok)
	assert.Equal(t, Datum{E: e, A: sys.DbIdent, V: String("test/ident")}, match)

	res = db.Write(req)
	assert.NoError(t, res.Error)
	assert.Equal(t, e, res.NewIDs[TempID("1")])
	assert.NotEqual(t, tx, res.ID)
	match, ok = res.Snapshot.Find(Claim{E: e, A: sys.DbIdent, V: String("test/ident")})
	assert.True(t, ok)
	assert.Equal(t, Datum{E: e, A: sys.DbIdent, V: String("test/ident")}, match)
}

func TestWriteAttr(t *testing.T) {
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
	assert.Positive(t, res.ID)
	id := res.NewIDs[TempID("1")]
	assert.Positive(t, id)
	assert.NotNil(t, res.Snapshot)
}

func TestEnforceValueUnique(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	req := Request{
		Claims: []*Claim{
			{E: TempID("1"), A: sys.DbIdent, V: String("person/name")},
			{E: TempID("1"), A: sys.AttrType, V: sys.AttrTypeString},
			{E: TempID("1"), A: sys.AttrUnique, V: sys.AttrUniqueValue},
			{E: TempID("2"), A: sys.DbIdent, V: String("person/age")},
			{E: TempID("2"), A: sys.AttrType, V: sys.AttrTypeInt},
			{E: TempID("3"), A: sys.DbIdent, V: String("person/score")},
			{E: TempID("3"), A: sys.AttrType, V: sys.AttrTypeFloat},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	req = Request{
		Claims: []*Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/age"), V: Int(49)},
			{E: TempID("1"), A: Ident("person/score"), V: Float(23.42)},
		},
	}
	res = db.Write(req)
	assert.NoError(t, res.Error)
	id := res.NewIDs[TempID("1")]
	assert.Positive(t, id)

	req = Request{
		Claims: []*Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/age"), V: Int(50)},
		},
	}
	res = db.Write(req)
	assert.Error(t, res.Error)
}

func TestIdentityUnique(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	req := Request{
		Claims: []*Claim{
			{E: TempID("1"), A: sys.DbIdent, V: String("person/name")},
			{E: TempID("1"), A: sys.AttrType, V: sys.AttrTypeString},
			{E: TempID("1"), A: sys.AttrUnique, V: sys.AttrUniqueIdentity},
			{E: TempID("2"), A: sys.DbIdent, V: String("person/age")},
			{E: TempID("2"), A: sys.AttrType, V: sys.AttrTypeInt},
			{E: TempID("3"), A: sys.DbIdent, V: String("person/score")},
			{E: TempID("3"), A: sys.AttrType, V: sys.AttrTypeFloat},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	req = Request{
		Claims: []*Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/age"), V: Int(49)},
			{E: TempID("1"), A: Ident("person/score"), V: Float(23.42)},
		},
	}
	res = db.Write(req)
	assert.NoError(t, res.Error)
	id := res.NewIDs[TempID("1")]
	assert.Positive(t, id)
	_, ok := res.Snapshot.Find(Claim{E: id, A: Ident("person/age"), V: Int(49)})
	assert.True(t, ok)

	req = Request{
		Claims: []*Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/age"), V: Int(50)},
		},
	}
	res = db.Write(req)
	assert.NoError(t, res.Error)
	assert.Equal(t, id, res.NewIDs[TempID("1")])
	_, ok = res.Snapshot.Find(Claim{E: id, A: Ident("person/age"), V: Int(50)})
	assert.True(t, ok)
	_, ok = res.Snapshot.Find(Claim{E: id, A: Ident("person/score"), V: Float(23.42)})
	assert.True(t, ok)
	_, ok = res.Snapshot.Find(Claim{E: LookupRef{A: Ident("person/name"), V: String("Donald")}, A: Ident("person/age"), V: Int(50)})
	assert.True(t, ok)
}
