// Package assembler provides for the construction of structs from sets of datums.
package assembler

import (
	"reflect"
	"time"

	"github.com/dball/destructive/internal/structs/attrs"
	. "github.com/dball/destructive/internal/types"
)

// Fact is a datum whose attribute has been resolved to an ident and has lost
// its transaction.
//
// TODO This is probably not a good idea, probably better to use datums in the context
// of a database but we haven't defined database yet, so.
type Fact struct {
	E ID
	A Ident
	V Value
}

type indexedAttrTag struct {
	attrs.AttrTag
	i int
}

// assembler assembles facts into entities.
type assembler[T any] struct {
	// pointers contains references to all entities allocated by this assembler.
	pointers map[ID]any
	// facts contains the facts not yet consumed, and must be sorted e-a-v.
	facts []Fact
	// base is a (nil) pointer to a struct of the type created for root entities.
	base *T
}

func NewAssembler[T any](base *T, facts []Fact) (a *assembler[T], err error) {
	ptr := reflect.ValueOf(base)
	if ptr.Kind() != reflect.Pointer {
		err = NewError("assembler.baseNotPointer")
		return
	}
	// TODO test that the pointer value type is a struct
	a = &assembler[T]{pointers: map[ID]any{}, facts: facts, base: base}
	return
}

func (a *assembler[T]) Next() (entity *T, err error) {
	// TODO return the next unreturned base entity if we found one via lookahead
	if len(a.facts) == 0 {
		return
	}
	// TODO wat seriously
	pp := reflect.New(reflect.TypeOf(a.base))
	ptr := pp.Elem()
	pp.Elem().Set(reflect.New(ptr.Type().Elem()))
	entity = pp.Elem().Interface().(*T)

	value := pp.Elem().Elem()
	typ := value.Type()
	n := typ.NumField()
	attrTags := make(map[Ident]indexedAttrTag, n)
	for i := 0; i < n; i++ {
		field := typ.Field(i)
		attrTag, attrErr := attrs.ParseAttrField(field)
		if attrErr != nil {
			err = attrErr
			return
		}
		if attrTag.Ident != "" {
			attrTags[attrTag.Ident] = indexedAttrTag{AttrTag: attrTag, i: i}
		}
	}
	var e ID
	for i, fact := range a.facts {
		if e == 0 {
			e = fact.E
			if e == 0 {
				err = NewError("assembler.zeroFactE")
				return
			}
			a.pointers[e] = entity
		} else if e != fact.E {
			// TODO we could have unresolved refs
			a.facts = a.facts[i:]
			return
		}
		attrTag, ok := attrTags[fact.A]
		if !ok {
			// We don't care if we get a fact with no field.
			continue
		}
		field := value.Field(attrTag.i)
		if attrTag.Pointer {
			// TODO who owns the vs anyway? If they're not copied at some point,
			// exposing value pointers opens the door to database corruption.
			switch v := fact.V.(type) {
			case String:
				// TODO does this count as a copy for the purpose of ensuring the
				// outer pointer doesn't change the Fact value?
				fv := string(v)
				field.Set(reflect.ValueOf(&fv))
			case Int:
				fv := int(v)
				field.Set(reflect.ValueOf(&fv))
			case Bool:
				fv := bool(v)
				field.Set(reflect.ValueOf(&fv))
			case Float:
				fv := float64(v)
				field.Set(reflect.ValueOf(&fv))
			case Inst:
				fv := time.Time(v)
				field.Set(reflect.ValueOf(&fv))
			default:
				err = NewError("assembler.invalidFactPointerValue")
				return
			}
		} else {
			switch v := fact.V.(type) {
			case String:
				field.SetString(string(v))
			case Int:
				// TODO what about overflows?
				field.SetInt(int64(v))
			case Bool:
				field.SetBool(bool(v))
			case Float:
				field.SetFloat(float64(v))
			case Inst:
				field.Set(reflect.ValueOf(time.Time(v)))
			default:
				err = NewError("assembler.invalidFactValue")
				return
			}
		}
	}
	a.facts = []Fact{}
	return
}
