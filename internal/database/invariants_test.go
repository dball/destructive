package database

import (
	"slices"
	"testing"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

// assertErrCode asserts err is a types.Error with the given code.
func assertErrCode(t *testing.T, err error, code string) {
	t.Helper()
	if !assert.Error(t, err) {
		return
	}
	e, ok := err.(Error)
	if assert.Truef(t, ok, "expected types.Error, got %T", err) {
		assert.Equal(t, code, e.Code)
	}
}

// newPersonDB returns a database with a small person schema: an identity-unique
// name (so it lands in the ave index) and a plain int age.
func newPersonDB(t *testing.T) Database {
	t.Helper()
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/name", Type: sys.AttrTypeString, Unique: sys.AttrUniqueIdentity},
		Attr{Ident: "person/age", Type: sys.AttrTypeInt},
	))
	return db
}

// TestWriteErrorCodes drives each single-request error path in the write pipeline
// and asserts the specific error code, locking down the rejection behavior.
func TestWriteErrorCodes(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T) (Database, Request)
		code  string
	}{
		{
			name: "invalid E ident",
			setup: func(t *testing.T) (Database, Request) {
				return NewIndexDatabase(32, 64, 64), Request{Claims: []Claim{
					{E: Ident("missing/entity"), A: sys.DbIdent, V: String("test/x")},
				}}
			},
			code: "database.write.invalidE",
		},
		{
			name: "invalid A ident",
			setup: func(t *testing.T) (Database, Request) {
				return NewIndexDatabase(32, 64, 64), Request{Claims: []Claim{
					{E: TempID("1"), A: Ident("missing/attr"), V: String("x")},
				}}
			},
			code: "database.write.invalidA",
		},
		{
			name: "invalid V ident",
			setup: func(t *testing.T) (Database, Request) {
				db := NewIndexDatabase(32, 64, 64)
				assert.NoError(t, Declare(db, Attr{Ident: "person/friend", Type: sys.AttrTypeRef}))
				return db, Request{Claims: []Claim{
					{E: TempID("1"), A: Ident("person/friend"), V: Ident("missing/target")},
				}}
			},
			code: "database.write.invalidV",
		},
		{
			name: "inconsistent AV type mismatch",
			setup: func(t *testing.T) (Database, Request) {
				db := NewIndexDatabase(32, 64, 64)
				assert.NoError(t, Declare(db, Attr{Ident: "person/age", Type: sys.AttrTypeInt}))
				return db, Request{Claims: []Claim{
					{E: TempID("1"), A: Ident("person/age"), V: String("not an int")},
				}}
			},
			code: "database.write.inconsistentAV",
		},
		{
			name: "invalid user ident",
			setup: func(t *testing.T) (Database, Request) {
				return NewIndexDatabase(32, 64, 64), Request{Claims: []Claim{
					{E: TempID("1"), A: sys.DbIdent, V: String("sys/sneaky")},
				}}
			},
			code: "database.write.invalidUserIdent",
		},
		{
			name: "attr requires ident",
			setup: func(t *testing.T) (Database, Request) {
				return NewIndexDatabase(32, 64, 64), Request{Claims: []Claim{
					{E: TempID("1"), A: sys.AttrType, V: sys.AttrTypeString},
				}}
			},
			code: "database.write.attrRequiresIdent",
		},
		{
			name: "invalid attr type",
			setup: func(t *testing.T) (Database, Request) {
				return NewIndexDatabase(32, 64, 64), Request{Claims: []Claim{
					{E: TempID("1"), A: sys.DbIdent, V: String("my/attr")},
					{E: TempID("1"), A: sys.AttrType, V: ID(99999)},
				}}
			},
			code: "database.write.invalidAttrType",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, req := c.setup(t)
			res := db.Write(req)
			assertErrCode(t, res.Error, c.code)
		})
	}
}

func TestWriteUniqueValueCollision(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db, Attr{Ident: "person/email", Type: sys.AttrTypeString, Unique: sys.AttrUniqueValue}))
	res := db.Write(Request{Claims: []Claim{{E: TempID("1"), A: Ident("person/email"), V: String("a@b.com")}}})
	assert.NoError(t, res.Error)
	res = db.Write(Request{Claims: []Claim{{E: TempID("2"), A: Ident("person/email"), V: String("a@b.com")}}})
	assertErrCode(t, res.Error, "database.write.uniqueValueCollision")
}

func TestWriteAttrImmutability(t *testing.T) {
	db := newPersonDB(t)
	ageID := db.Read().ResolveIdent(Ident("person/age"))
	assert.Positive(t, ageID)

	t.Run("type change disallowed", func(t *testing.T) {
		res := db.Write(Request{Claims: []Claim{{E: ageID, A: sys.AttrType, V: sys.AttrTypeString}}})
		assertErrCode(t, res.Error, "database.write.attrTypeChangeDisallowed")
	})
	t.Run("type retract disallowed", func(t *testing.T) {
		res := db.Write(Request{Claims: []Claim{{E: ageID, A: sys.AttrType, V: sys.AttrTypeInt, Retract: true}}})
		assertErrCode(t, res.Error, "database.write.attrRetractDisallowed")
	})
}

// TestTempIDResolution confirms a tempid used in multiple claims of one request
// resolves to a single new entity id.
func TestTempIDResolution(t *testing.T) {
	db := newPersonDB(t)
	res := db.Write(Request{Claims: []Claim{
		{E: TempID("p"), A: Ident("person/name"), V: String("Ada")},
		{E: TempID("p"), A: Ident("person/age"), V: Int(36)},
	}})
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("p")]
	assert.Positive(t, id)
	assert.Len(t, res.TempIDs, 1)
	assert.Equal(t, 2, res.Snapshot.Count(Claim{E: id}))
}

// TestLookupRefResolution exercises resolving an entity by a unique attribute value
// (success) and the failure path when no entity matches.
func TestLookupRefResolution(t *testing.T) {
	db := newPersonDB(t)
	res := db.Write(Request{Claims: []Claim{{E: TempID("p"), A: Ident("person/name"), V: String("Grace")}}})
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("p")]

	// Resolve the existing entity by name and attach an age to it.
	res = db.Write(Request{Claims: []Claim{
		{E: LookupRef{A: Ident("person/name"), V: String("Grace")}, A: Ident("person/age"), V: Int(85)},
	}})
	assert.NoError(t, res.Error)
	ok := res.Snapshot.Has(Claim{E: id, A: Ident("person/age"), V: Int(85)})
	assert.True(t, ok)

	// A lookup ref that matches nothing is an invalid entity reference.
	res = db.Write(Request{Claims: []Claim{
		{E: LookupRef{A: Ident("person/name"), V: String("Nobody")}, A: Ident("person/age"), V: Int(1)},
	}})
	assertErrCode(t, res.Error, "database.write.invalidE")
}

func TestCardinalityOneReplaces(t *testing.T) {
	db := newPersonDB(t)
	res := db.Write(Request{Claims: []Claim{
		{E: TempID("p"), A: Ident("person/name"), V: String("Lin")},
		{E: TempID("p"), A: Ident("person/age"), V: Int(40)},
	}})
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("p")]

	res = db.Write(Request{Claims: []Claim{{E: id, A: Ident("person/age"), V: Int(41)}}})
	assert.NoError(t, res.Error)

	view := res.Snapshot
	assert.Equal(t, 1, view.Count(Claim{E: id, A: Ident("person/age")}), "cardinality-one keeps a single value")
	ok := view.Has(Claim{E: id, A: Ident("person/age"), V: Int(41)})
	assert.True(t, ok)
	ok = view.Has(Claim{E: id, A: Ident("person/age"), V: Int(40)})
	assert.False(t, ok, "old value replaced")
}

func TestCardinalityManyAccumulates(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/nick", Type: sys.AttrTypeString, Cardinality: sys.AttrCardinalityMany},
	))
	res := db.Write(Request{Claims: []Claim{
		{E: TempID("p"), A: Ident("person/nick"), V: String("Red")},
		{E: TempID("p"), A: Ident("person/nick"), V: String("Ace")},
	}})
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("p")]
	assert.Equal(t, 2, res.Snapshot.Count(Claim{E: id, A: Ident("person/nick")}), "cardinality-many keeps all values")
}

func TestRetractMultiple(t *testing.T) {
	db := newPersonDB(t)
	res := db.Write(Request{Claims: []Claim{
		{E: TempID("a"), A: Ident("person/name"), V: String("Ann")},
		{E: TempID("b"), A: Ident("person/name"), V: String("Bo")},
	}})
	assert.NoError(t, res.Error)

	res = db.Write(Request{Retractions: []Retraction{
		{Constraints: map[IDRef]Void{LookupRef{A: Ident("person/name"), V: String("Ann")}: {}}},
		{Constraints: map[IDRef]Void{LookupRef{A: Ident("person/name"), V: String("Bo")}: {}}},
	}})
	assert.NoError(t, res.Error)
	ok := res.Snapshot.Has(Claim{E: LookupRef{A: Ident("person/name"), V: String("Ann")}, A: Ident("person/name"), V: String("Ann")})
	assert.False(t, ok)
	ok = res.Snapshot.Has(Claim{E: LookupRef{A: Ident("person/name"), V: String("Bo")}, A: Ident("person/name"), V: String("Bo")})
	assert.False(t, ok)
}

// TestRetractDoesNotCascadeToReferences confirms that retracting an entity removes
// only its own datums, leaving entities it references through ordinary reference
// attributes untouched. This is correct: friendship is a plain ref, not a dependent
// ref. The Retraction doc's "recursively" applies only to dependent references
// (sys/attr/ref/type/dependent), which are not yet implemented; see the planned
// extensions note in doc/architecture.md.
func TestRetractDoesNotCascadeToReferences(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db,
		Attr{Ident: "person/name", Type: sys.AttrTypeString, Unique: sys.AttrUniqueIdentity},
		Attr{Ident: "person/friend", Type: sys.AttrTypeRef},
	))
	res := db.Write(Request{Claims: []Claim{{E: TempID("b"), A: Ident("person/name"), V: String("Bob")}}})
	assert.NoError(t, res.Error)
	bob := res.TempIDs[TempID("b")]
	res = db.Write(Request{Claims: []Claim{
		{E: TempID("a"), A: Ident("person/name"), V: String("Alice")},
		{E: TempID("a"), A: Ident("person/friend"), V: bob},
	}})
	assert.NoError(t, res.Error)

	res = db.Write(Request{Retractions: []Retraction{
		{Constraints: map[IDRef]Void{LookupRef{A: Ident("person/name"), V: String("Alice")}: {}}},
	}})
	assert.NoError(t, res.Error)
	// Bob, whom Alice referenced through a plain ref, is correctly untouched.
	ok := res.Snapshot.Has(Claim{E: bob, A: Ident("person/name"), V: String("Bob")})
	assert.True(t, ok, "referenced entity survives retraction of the referrer")
}

// TestSnapshotIsolation confirms a snapshot taken before a write is unaffected by
// the write — the core "apply changes without affecting current readers" goal.
func TestSnapshotIsolation(t *testing.T) {
	db := newPersonDB(t)
	res := db.Write(Request{Claims: []Claim{
		{E: TempID("p"), A: Ident("person/name"), V: String("Mae")},
		{E: TempID("p"), A: Ident("person/age"), V: Int(30)},
	}})
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("p")]
	before := res.Snapshot

	res = db.Write(Request{Claims: []Claim{{E: id, A: Ident("person/age"), V: Int(31)}}})
	assert.NoError(t, res.Error)
	after := res.Snapshot

	ok := before.Has(Claim{E: id, A: Ident("person/age"), V: Int(30)})
	assert.True(t, ok, "old snapshot still sees old value")
	ok = before.Has(Claim{E: id, A: Ident("person/age"), V: Int(31)})
	assert.False(t, ok, "old snapshot does not see the later write")
	ok = after.Has(Claim{E: id, A: Ident("person/age"), V: Int(31)})
	assert.True(t, ok, "new snapshot sees new value")
}

// TestWriteErrorLeavesDatabaseIntact confirms a rejected write zeroes the response
// and does not mutate the database.
func TestWriteErrorLeavesDatabaseIntact(t *testing.T) {
	db := newPersonDB(t)
	res := db.Write(Request{Claims: []Claim{{E: TempID("p"), A: Ident("person/age"), V: Int(20)}}})
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("p")]

	bad := db.Write(Request{Claims: []Claim{{E: id, A: Ident("person/age"), V: String("nope")}}})
	assertErrCode(t, bad.Error, "database.write.inconsistentAV")
	assert.Zero(t, bad.ID)
	assert.Nil(t, bad.TempIDs)

	// The earlier datum is still present and further writes still succeed.
	ok := db.Read().Has(Claim{E: id, A: Ident("person/age"), V: Int(20)})
	assert.True(t, ok)
	ok2 := db.Write(Request{Claims: []Claim{{E: TempID("q"), A: Ident("person/name"), V: String("Quinn")}}})
	assert.NoError(t, ok2.Error)
}

// TestSnapshotQueryRouting locks down the snapshot query surface across every shape
// of claim: indexed shapes, value-filtered shapes, and the unconstrained scan.
func TestSnapshotQueryRouting(t *testing.T) {
	db := newPersonDB(t)
	res := db.Write(Request{Claims: []Claim{
		{E: TempID("p"), A: Ident("person/name"), V: String("Donald")},
		{E: TempID("p"), A: Ident("person/age"), V: Int(49)},
	}})
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("p")]
	tx := res.ID
	view := res.Snapshot
	ageA := view.ResolveIdent(Ident("person/age"))
	ageDatum := Datum{E: id, A: ageA, V: Int(49), T: tx}

	t.Run("indexed shapes", func(t *testing.T) {
		// (E,*,*) all datums for the entity
		assert.Len(t, slices.Collect(view.Select(Claim{E: id})), 2)
		assert.Equal(t, 2, view.Count(Claim{E: id}))
		// (E,A,*)
		assert.Len(t, slices.Collect(view.Select(Claim{E: id, A: Ident("person/age")})), 1)
		assert.Equal(t, 1, view.Count(Claim{E: id, A: Ident("person/age")}))
		// (*,A,V) via the unique ave index
		assert.Len(t, slices.Collect(view.Select(Claim{A: Ident("person/name"), V: String("Donald")})), 1)
		assert.Equal(t, 1, view.Count(Claim{A: Ident("person/name"), V: String("Donald")}))
		// (*,A,*) via the aev index
		assert.Len(t, slices.Collect(view.Select(Claim{A: Ident("person/age")})), 1)
		assert.Equal(t, 1, view.Count(Claim{A: Ident("person/age")}))
	})

	t.Run("filtered shapes", func(t *testing.T) {
		// (E,A,V) present and absent
		assert.Equal(t, []Datum{ageDatum}, slices.Collect(view.Select(Claim{E: id, A: Ident("person/age"), V: Int(49)})))
		assert.Empty(t, slices.Collect(view.Select(Claim{E: id, A: Ident("person/age"), V: Int(999)})))
		assert.Equal(t, 1, view.Count(Claim{E: id, A: Ident("person/age"), V: Int(49)}))
		// (E,*,V): the entity's datums filtered by value
		assert.Equal(t, []Datum{ageDatum}, slices.Collect(view.Select(Claim{E: id, V: Int(49)})))
		// (*,*,V): every datum with that value (scans all, including system datums)
		assert.Equal(t, []Datum{ageDatum}, slices.Collect(view.Select(Claim{V: Int(49)})))
		assert.Equal(t, 1, view.Count(Claim{V: Int(49)}))
	})

	t.Run("all datums", func(t *testing.T) {
		all := slices.Collect(view.Select(Claim{}))
		assert.Equal(t, view.Count(Claim{}), len(all))
		assert.Contains(t, all, ageDatum)
		assert.Greater(t, len(all), 2, "includes system datums plus the user datums")
	})
}

// TestSnapshotSelectNonUniqueAV covers the (*,A,V) path for a non-unique attribute,
// which is not in the ave index and so is served by scanning the attribute and
// filtering by value.
func TestSnapshotSelectNonUniqueAV(t *testing.T) {
	db := NewIndexDatabase(32, 64, 64)
	assert.NoError(t, Declare(db, Attr{Ident: "person/age", Type: sys.AttrTypeInt}))
	res := db.Write(Request{Claims: []Claim{
		{E: TempID("a"), A: Ident("person/age"), V: Int(40)},
		{E: TempID("b"), A: Ident("person/age"), V: Int(41)},
		{E: TempID("c"), A: Ident("person/age"), V: Int(40)},
	}})
	assert.NoError(t, res.Error)
	view := res.Snapshot
	assert.Equal(t, 2, view.Count(Claim{A: Ident("person/age"), V: Int(40)}))
	assert.Equal(t, 1, view.Count(Claim{A: Ident("person/age"), V: Int(41)}))
}
