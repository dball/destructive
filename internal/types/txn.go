package types

import "time"

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

// Request is a set of claims and constraints on their temporary ids.
type Request struct {
	Claims  []*Claim
	TempIDs map[TempID]map[IDRef]Void
}

// Transaction is the result of successfully applying a request to the database.
type Transaction struct {
	ID       ID
	NewIDs   map[TempID]ID
	Database Database
}

type Database interface{}
