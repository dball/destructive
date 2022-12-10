// Package types defines the core system types.
package types

import (
	"fmt"
	"reflect"
	"time"
)

// Void is used for values in maps used as sets.
type Void struct{}

// Value is an immutable scalar. Nil is not a valid value.
type Value interface {
	IsEmpty() bool
}

// ID is issued by the system and never reused. 0 is not a valid id.
type ID uint64

func (id ID) String() string {
	return fmt.Sprintf("#id(%d)", uint64(id))
}

// String is a string.
type String string

func (s String) String() string {
	return fmt.Sprintf("#str(\"%s\"", string(s))
}

// Int is a signed integer.
type Int int64

func (i Int) String() string {
	return fmt.Sprintf("#int(%d)", int64(i))
}

// Bool is a boolean.
type Bool bool

func (b Bool) String() string {
	if bool(b) {
		return "#t"
	} else {
		return "#f"
	}
}

// Inst is a instant in time.
type Inst time.Time

func (inst Inst) String() string {
	return fmt.Sprintf("#inst(\"%s\")", time.Time(inst).Format(time.RFC3339))
}

// Float is a floating-point number.
type Float float64

func (f Float) String() string {
	return fmt.Sprintf("#float(%v)", float64(f))
}

// TimeType is the type of golang's Time value.
var TimeType = reflect.TypeOf(time.Time{})

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

func (d Datum) String() string {
	return fmt.Sprintf("#d[%s, %s, %s, %s]", d.E, d.A, d.V, d.T)
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
