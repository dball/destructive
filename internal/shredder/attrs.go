package shredder

import (
	"reflect"
	"strings"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

// AttrTag is a partial representation of an attr and hints on how
// how it is realized on a struct field
type AttrTag struct {
	Ident       Ident
	Unique      ID
	Type        ID
	IgnoreEmpty bool
	Pointer     bool
	MapKey      Ident
	CollValue   Ident
}

func parseAttrTag(tag string) (attr AttrTag, err error) {
	parts := strings.Split(tag, ",")
	attr.Ident = Ident(parts[0])
	n := len(parts)
	for i := 1; i < n; i++ {
		part := parts[i]
		switch part {
		case "identity":
			if attr.Unique != 0 {
				err = NewError("shredder.duplicateUniqueDirective", "tag", tag)
				return
			}
			attr.Unique = sys.AttrUniqueIdentity
		case "unique":
			if attr.Unique != 0 {
				err = NewError("shredder.duplicateUniqueDirective", "tag", tag)
				return
			}
			attr.Unique = sys.AttrUniqueValue
		case "ignoreempty":
			attr.IgnoreEmpty = true
		default:
			switch {
			case strings.HasPrefix(part, "key="):
				attr.MapKey = Ident(part[4:])
			case strings.HasPrefix(part, "value="):
				attr.CollValue = Ident(part[6:])
			default:
				err = NewError("shredder.invalidDirective", "tag", tag)
				return
			}
		}
	}
	return
}

// ParseAttrField parses an attribute from the struct field. If the field
// does not define an attribute, it's ident will be empty.
//
// Note if the tag string needs to vary, we could define this on the shredder
// and give it as an arg.
func ParseAttrField(field reflect.StructField) (attr AttrTag, err error) {
	tag, ok := field.Tag.Lookup("attr")
	if !ok {
		return
	}
	attr, err = parseAttrTag(tag)
	if err != nil {
		return
	}
	if attr.Ident == sys.DbId {
		return
	}
	switch field.Type.Kind() {
	case reflect.Bool:
		attr.Type = sys.AttrTypeBool
	case reflect.Int:
		attr.Type = sys.AttrTypeInt
	case reflect.String:
		attr.Type = sys.AttrTypeString
	case reflect.Float64:
		attr.Type = sys.AttrTypeFloat
	case reflect.Struct:
		if TimeType == field.Type {
			attr.Type = sys.AttrTypeInst
		} else {
			attr.Type = sys.AttrTypeRef
		}
	case reflect.Map:
		attr.Type = sys.AttrRefType
		if attr.MapKey == "" {
			attr.MapKey = Ident(sys.DbId)
		}
	case reflect.Slice:
		attr.Type = sys.AttrRefType
	case reflect.Pointer:
		attr.Pointer = true
		// This repeats the outer switch, but without the pointer case.
		// TODO map, slice, right?
		switch field.Type.Elem().Kind() {
		case reflect.Bool:
			attr.Type = sys.AttrTypeBool
		case reflect.Int:
			attr.Type = sys.AttrTypeInt
		case reflect.String:
			attr.Type = sys.AttrTypeString
		case reflect.Float64:
			attr.Type = sys.AttrTypeFloat
		case reflect.Struct:
			if TimeType == field.Type {
				attr.Type = sys.AttrTypeInst
			} else {
				attr.Type = sys.AttrTypeRef
			}
		default:
			err = NewError("shredder.invalidPointerType", "tag", tag, "type", field.Type, "kind", field.Type.Elem().Kind())
		}
	default:
		err = NewError("shredder.invalidType", "tag", tag, "type", field.Type, "kind", field.Type.Kind())
	}
	return
}
