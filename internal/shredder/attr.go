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
	ident       Ident
	unique      ID
	typ         ID
	ignoreEmpty bool
	pointer     bool
	mapKey      Ident
}

func parseAttrTag(tag string) (attr attrTag, err error) {
	parts := strings.Split(tag, ",")
	attr.ident = Ident(parts[0])
	n := len(parts)
	for i := 1; i < n; i++ {
		part := parts[i]
		switch part {
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
		case "ignoreempty":
			attr.ignoreEmpty = true
		default:
			if strings.HasPrefix(part, "key=") {
				attr.mapKey = Ident(part[4:])
			} else {
				err = NewError("shredder.invalidDirective", "tag", tag)
				return
			}
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
	case reflect.Map:
		attr.typ = sys.AttrRefType
		if attr.mapKey == "" {
			attr.mapKey = Ident(sys.DbId)
		}
	case reflect.Pointer:
		attr.pointer = true
		// This repeats the outer switch, but without the pointer case.
		switch field.Type.Elem().Kind() {
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
			err = NewError("shredder.invalidPointerType", "tag", tag, "type", field.Type, "kind", field.Type.Elem().Kind())
		}
	default:
		err = NewError("shredder.invalidType", "tag", tag, "type", field.Type, "kind", field.Type.Kind())
	}
	return
}
