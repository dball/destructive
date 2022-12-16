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
	assert.Equal(t, map[TempID]ID{TempID("1"): sys.FirstUserID + 1}, res.TempIDs)
	assert.NotNil(t, res.Snapshot)
	e = res.TempIDs[TempID("1")]
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
	assert.Equal(t, e, res.TempIDs[TempID("1")])
	assert.NotEqual(t, tx, res.ID)
	match, ok = res.Snapshot.Find(Claim{E: e, A: sys.DbIdent, V: String("test/ident")})
	assert.True(t, ok)
	assert.Equal(t, Datum{E: e, A: sys.DbIdent, V: String("test/ident")}, match)
}

func TestWriteAttr(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/name", Type: sys.AttrTypeString, Unique: sys.AttrUniqueIdentity},
	))
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/name", Type: sys.AttrTypeString, Unique: sys.AttrUniqueIdentity},
	))

	req := Request{
		Claims: []*Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	assert.Positive(t, res.ID)
	id := res.TempIDs[TempID("1")]
	assert.Positive(t, id)
	assert.NotNil(t, res.Snapshot)
}

func TestEnforceValueUnique(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/name", Type: sys.AttrTypeString, Unique: sys.AttrUniqueValue},
		Attr{Ident: "person/age", Type: sys.AttrTypeInt},
		Attr{Ident: "person/score", Type: sys.AttrTypeFloat},
	))

	req := Request{
		Claims: []*Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/age"), V: Int(49)},
			{E: TempID("1"), A: Ident("person/score"), V: Float(23.42)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("1")]
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
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/name", Type: sys.AttrTypeString, Unique: sys.AttrUniqueIdentity},
		Attr{Ident: "person/age", Type: sys.AttrTypeInt},
		Attr{Ident: "person/score", Type: sys.AttrTypeFloat},
	))
	req := Request{
		Claims: []*Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/age"), V: Int(49)},
			{E: TempID("1"), A: Ident("person/score"), V: Float(23.42)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("1")]
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
	assert.Equal(t, id, res.TempIDs[TempID("1")])
	_, ok = res.Snapshot.Find(Claim{E: id, A: Ident("person/age"), V: Int(50)})
	assert.True(t, ok)
	_, ok = res.Snapshot.Find(Claim{E: id, A: Ident("person/score"), V: Float(23.42)})
	assert.True(t, ok)
	_, ok = res.Snapshot.Find(Claim{E: LookupRef{A: Ident("person/name"), V: String("Donald")}, A: Ident("person/age"), V: Int(50)})
	assert.True(t, ok)
}

func TestSelect(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/name", Type: sys.AttrTypeString, Unique: sys.AttrUniqueIdentity},
		Attr{Ident: "person/age", Type: sys.AttrTypeInt},
		Attr{Ident: "person/score", Type: sys.AttrTypeFloat},
	))
	req := Request{
		Claims: []*Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/age"), V: Int(49)},
			{E: TempID("1"), A: Ident("person/score"), V: Float(23.42)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("1")]
	data := res.Snapshot.Select(Claim{E: id}).Drain()
	// TODO the A 0's aren't right. How do we lookup by ident?
	assert.Equal(t, []Datum{
		{E: id, A: 0, V: String("Donald")},
		{E: id, A: 0, V: Int(49)},
		{E: id, A: 0, V: Float(23.42)},
	}, data)
}

func TestRetract(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/name", Type: sys.AttrTypeString, Unique: sys.AttrUniqueIdentity},
		Attr{Ident: "person/age", Type: sys.AttrTypeInt},
		Attr{Ident: "person/score", Type: sys.AttrTypeFloat},
	))
	req := Request{
		Claims: []*Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/age"), V: Int(49)},
			{E: TempID("1"), A: Ident("person/score"), V: Float(23.42)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	req = Request{
		Retractions: []*Retraction{
			{Constraints: map[IDRef]Void{LookupRef{A: Ident("person/name"), V: String("Donald")}: {}}},
		},
	}
	res = db.Write(req)
	assert.NoError(t, res.Error)
	_, ok := res.Snapshot.Find(Claim{E: LookupRef{A: Ident("person/name"), V: String("Donald")}, A: Ident("person/age"), V: Int(49)})
	assert.False(t, ok)
}
