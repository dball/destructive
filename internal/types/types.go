// Package types defines the core system types.
package types

import "time"

// Void is used for values in maps used as sets.
type Void struct{}

// Value is an immutable scalar. Nil is not a valid value.
type Value interface {
	IsEmpty() bool
}

// IDs are issued by the system and never reused. 0 is not a valid id.
type ID uint64

// String is a string.
type String string

// Int is a signed integer.
type Int int64

// Bool is a boolean.
type Bool bool

// Inst is a instant in time.
type Inst time.Time

// Float is a floating-point number.
type Float float64

// These are the system values.

func (x ID) IsEmpty() bool     { return uint64(x) == 0 }
func (x String) IsEmpty() bool { return string(x) == "" }
func (x Int) IsEmpty() bool    { return int64(x) == 0 }
func (x Bool) IsEmpty() bool   { return !bool(x) }
func (x Inst) IsEmpty() bool   { return time.Time(x).IsZero() }
func (x Float) IsEmpty() bool  { return float64(x) == 0 }

// Datum is the fundamental data model.
type Datum struct {
	// E is the entity id.
	E ID
	// A is the attribute id.
	A ID
	// V is the value.
	V Value
	// T is the transaction id.
	T ID
}

// D is a convenience function for building a datum.
func D(e ID, a ID, v Value, t ID) Datum {
	return Datum{e, a, v, t}
}

// Ident is a globally unique system identifier for an entity, generally used for attributes.
type Ident string

// IDRef is a value that may resolve to an attribute id.
type IDRef interface {
	IsIDRef()
}

func (ID) IsIDRef()    {}
func (Ident) IsIDRef() {}

// LookupRef is a combination of a unique attribute ref and a value, which may resolve to an entity id.
type LookupRef struct {
	// A may be any form of IDRef, most commonly an Ident.
	A IDRef
	V Value
}

func (LookupRef) IsIDRef() {}
