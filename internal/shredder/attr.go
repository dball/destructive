package shredder

import (
	"reflect"
	"strings"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

// attrTags is a partial representation of an attr and hints on how
// how it is realized on a struct field
type attrTag struct {
	ident  Ident
	unique ID
	typ    ID
}

func parseAttrTag(tag string) (attr attrTag, err error) {
	parts := strings.Split(tag, ",")
	attr.ident = Ident(parts[0])
	n := len(parts)
	for i := 1; i < n; i++ {
		switch parts[i] {
		case "identity":
			if attr.unique != 0 {
				err = NewError("shredder.duplicateUniqueDirective", "tag", tag)
				return
			}
			attr.unique = sys.AttrUniqueIdentity
		case "unique":
			if attr.unique != 0 {
				err = NewError("shredder.duplicateUniqueDirective", "tag", tag)
				return
			}
			attr.unique = sys.AttrUniqueValue
		default:
			err = NewError("shredder.invalidDirective", "tag", tag)
			return
		}
	}
	return
}

// parseAttrField parses an attribute from the struct field. If the field
// does not define an attribute, it's ident will be empty.
//
// Note if the tag string needs to vary, we could define this on the shredder.
func parseAttrField(field reflect.StructField) (attr attrTag, err error) {
	tag, ok := field.Tag.Lookup("attr")
	if !ok {
		return
	}
	attr, err = parseAttrTag(tag)
	if err != nil {
		return
	}
	if attr.ident == sys.DbId {
		return
	}
	switch field.Type.Kind() {
	case reflect.Bool:
		attr.typ = sys.AttrTypeBool
	case reflect.Int:
		attr.typ = sys.AttrTypeInt
	case reflect.String:
		attr.typ = sys.AttrTypeString
	case reflect.Float64:
		attr.typ = sys.AttrTypeFloat
	case reflect.Struct:
		if TimeType == field.Type {
			attr.typ = sys.AttrTypeInst
		} else {
			attr.typ = sys.AttrTypeRef
		}
	default:
		panic("Invalid attr field type")
	}
	return
}
