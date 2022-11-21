// Package assembler provides for the construction of structs from sets of datums.
package assembler

import (
	"reflect"

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

func Assemble(target any, facts []Fact) (err error) {
	ptr := reflect.ValueOf(target)
	if ptr.Kind() != reflect.Pointer {
		err = NewError("assembler.targetNotPointer")
		return
	}
	value := ptr.Elem()
	if value.Kind() != reflect.Struct {
		err = NewError("assembler.targetValueNotStruct")
		return
	}
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
		attrTags[attrTag.Ident] = indexedAttrTag{AttrTag: attrTag, i: i}
	}
	var e ID
	for _, fact := range facts {
		if e == 0 {
			e = fact.E
			if e == 0 {
				err = NewError("assembler.zeroFactE")
				return
			}
		} else if e != fact.E {
			err = NewError("assembler.invalidFactE", "e", fact.E)
			return
		}
		attrTag, ok := attrTags[fact.A]
		if !ok {
			// We don't care if we get a fact with no field.
			continue
		}
		field := value.Field(attrTag.i)
		// TODO here we're allowing the type of datum value drive the field type.
		// Presumably, this should be controlled by the attrTag struct, which
		// considers the field type.
		if attrTag.Pointer {
			// TODO who owns the vs anyway? If they're not copied at some point,
			// exposing value pointers opens the door to database corruption.
			switch v := fact.V.(type) {
			case String:
				// TODO does this count as a copy for the purpose of ensuring the
				// outer pointer doesn't change the Fact value?
				s := string(v)
				field.Set(reflect.ValueOf(&s))
			default:
				err = NewError("assembler.invalidFactV")
				return
			}
		} else {
			switch v := fact.V.(type) {
			case String:
				field.SetString(string(v))
			default:
				err = NewError("assembler.invalidFactV")
				return
			}
		}
	}
	return
}
