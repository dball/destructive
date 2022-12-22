package database

import (
	"testing"
	"time"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestWriteSimple(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	var e, tx ID
	req := Request{
		Claims: []Claim{
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
		Claims: []Claim{
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
		Claims: []Claim{
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
		Claims: []Claim{
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
		Claims: []Claim{
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
		Claims: []Claim{
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
		Claims: []Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/age"), V: Int(49)},
			{E: TempID("1"), A: Ident("person/score"), V: Float(23.42)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	tx := res.ID
	id := res.TempIDs[TempID("1")]
	view := res.Snapshot
	data := view.Select(Claim{E: id}).Drain()
	assert.Equal(t, []Datum{
		{E: id, A: view.ResolveIdent(Ident("person/name")), V: String("Donald"), T: tx},
		{E: id, A: view.ResolveIdent(Ident("person/age")), V: Int(49), T: tx},
		{E: id, A: view.ResolveIdent(Ident("person/score")), V: Float(23.42), T: tx},
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
		Claims: []Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/age"), V: Int(49)},
			{E: TempID("1"), A: Ident("person/score"), V: Float(23.42)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	req = Request{
		Retractions: []Retraction{
			{Constraints: map[IDRef]Void{LookupRef{A: Ident("person/name"), V: String("Donald")}: {}}},
		},
	}
	res = db.Write(req)
	assert.NoError(t, res.Error)
	_, ok := res.Snapshot.Find(Claim{E: LookupRef{A: Ident("person/name"), V: String("Donald")}, A: Ident("person/age"), V: Int(49)})
	assert.False(t, ok)
}

func TestBool(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/likes-pickles", Type: sys.AttrTypeBool},
	))
	req := Request{
		Claims: []Claim{
			{E: TempID("1"), A: Ident("person/likes-pickles"), V: Bool(true)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("1")]
	view := res.Snapshot
	tx := res.ID
	data := view.Select(Claim{E: id, A: Ident("person/likes-pickles")}).Drain()
	assert.Equal(t, []Datum{
		{E: id, A: view.ResolveIdent(Ident("person/likes-pickles")), V: Bool(true), T: tx},
	}, data)
}

func TestInst(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/born", Type: sys.AttrTypeInst},
	))
	born := time.Date(1969, 7, 20, 20, 17, 54, 0, time.UTC)
	req := Request{
		Claims: []Claim{
			{E: TempID("1"), A: Ident("person/born"), V: Inst(born)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("1")]
	view := res.Snapshot
	tx := res.ID
	data := view.Select(Claim{E: id, A: Ident("person/born")}).Drain()
	assert.Equal(t, []Datum{
		{E: id, A: view.ResolveIdent(Ident("person/born")), V: Inst(born), T: tx},
	}, data)
}
