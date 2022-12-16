package types

import (
	"time"

	"github.com/dball/destructive/internal/iterator"
)

// TempID is a value that will resolve to a system id when a claim is asserted.
type TempID string

// TxnID is a value that will resolve to the transaction id when a claim is asserted.
type TxnID struct{}

// ERef is a value that will resolve to an entity id when a claim is asserted or retracted.
type ERef interface {
	IsERef()
}

func (ID) IsERef()        {}
func (Ident) IsERef()     {}
func (LookupRef) IsERef() {}
func (TempID) IsERef()    {}
func (TxnID) IsERef()     {}

// VRef is a value that will resolve to a value when a claim is asserted or restracted.
type VRef interface {
	IsVRef()
}

func (ID) IsVRef()        {}
func (Ident) IsVRef()     {}
func (String) IsVRef()    {}
func (Int) IsVRef()       {}
func (Bool) IsVRef()      {}
func (Inst) IsVRef()      {}
func (Float) IsVRef()     {}
func (TempID) IsVRef()    {}
func (LookupRef) IsVRef() {}

func ToVRef(x any) (v VRef, ok bool) {
	ok = true
	switch xv := x.(type) {
	case ID:
		v = xv
	case String:
		v = xv
	case Int:
		v = xv
	case Bool:
		v = xv
	case Inst:
		v = xv
	case Float:
		v = xv
	case TempID:
		v = xv
	case LookupRef:
		v = xv
	case uint64:
		// TODO idk if I like this blanket policy tbh
		v = ID(xv)
	case string:
		v = String(xv)
	case int64:
		v = Int(xv)
	case time.Time:
		v = Inst(xv)
	case float64:
		v = Float(xv)
	default:
		ok = false
	}
	return
}

// Claim is an assertion of or retraction of a datum, or all datums for an entity.
type Claim struct {
	E       ERef
	A       IDRef
	V       VRef
	Retract bool
}

// Retraction is a retraction of all attribute values for an entity as well as
// all of its dependent references, recursively. The constraints must resolve to a
// single id or the retraction is rejected.
type Retraction struct {
	Constraints map[IDRef]Void
}

// Request is a set of claims and constraints on their temporary ids.
type Request struct {
	// The list of claims.
	Claims []*Claim
	// The list of retractions.
	Retractions []*Retraction
}

// Response is the result of trying to apply a request to the database.
type Response struct {
	// ID is the id of the transaction which applied the datums, if successful.
	ID ID
	// TempIDs is a map of entity ids indexed by their referring tempids, if successful.
	TempIDs map[TempID]ID
	// Snapshot is the value of the database after applying the datums, or
	// which rejected the datums.
	Snapshot Snapshot
	// Error describes why the datums could not be applied.
	Error error
}

// Database is a mutable set of datums. Databases are safe for concurrent use.
type Database interface {
	// Read returns a snapshot of the current state of the database.
	Read() Snapshot
	// Write tries to apply the request to the database.
	Write(req Request) Response
}

// Snapshot is an immutable set of datums. Snapshots are safe for concurrent use.
type Snapshot interface {
	// Select returns an iterator of datums matching the claim. Empty values in the
	// claim's fields indicate all values will match.
	Select(claim Claim) *iterator.Iterator[Datum]
	// Find returns the datum matching the claim, if any.
	Find(claim Claim) (match Datum, found bool)
	// ResolveIdent resolves an ident to an id.
	ResolveIdent(ident Ident) (id ID)
}
