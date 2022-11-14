package shredder

import (
	"reflect"
	"strings"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

func parseAttrTag(tag string) (attr Attr) {
	parts := strings.Split(tag, ",")
	// TODO validate parts, panic
	attr.Ident = Ident(parts[0])
	if len(parts) > 1 {
		switch parts[1] {
		case "identity":
			attr.Unique = sys.AttrUniqueIdentity
		case "unique":
			attr.Unique = sys.AttrUniqueValue
		}
	}
	return
}

// parseAttrField parses an attribute from the struct field. If the field
// does not define an attribute, it's ident will be empty.
//
// Note if the tag string needs to vary, we could define this on the shredder.
func parseAttrField(field reflect.StructField) (attr Attr) {
	tag, ok := field.Tag.Lookup("attr")
	if !ok {
		return
	}
	attr = parseAttrTag(tag)
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
	default:
		panic("Invalid attr field type")
	}
	return
}
