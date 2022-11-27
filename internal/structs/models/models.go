// Package models provides models of structs with datum bindings.
package models

import (
	"reflect"
	"strings"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

// StructModel models a struct that has fields bound to attributes, whose instances
// correspond to entities.
type StructModel struct {
	// Type is the struct type, whose kind must be a struct.
	Type reflect.Type
	// AttrFields are the fields bound to attributes, indexed by ident.
	AttrFields map[Ident]AttrFieldModel
}

// AttrFieldModel models a field bound to an attribute.
type AttrFieldModel struct {
	// Index is the position of the field in the struct.
	Index int
	// FieldType is the field's go type.
	FieldType reflect.Type
	// Unique is the ID of the uniqueness ident. This may be zero.
	Unique ID
	// Type is the ID of the type ident. This may not be zero.
	Type ID
	// IgnoreEmpty indicates that zero values are treated as nils.
	IgnoreEmpty bool
	// MapKey is the ident for the keys of this field's map entries in the child entities.
	MapKey Ident
	// CollValue is the ident for the scalar values in this field's slice entries.
	CollValue Ident
}

// IsMap indicates that the field value is a map.
func (attr AttrFieldModel) IsMap() bool {
	return attr.FieldType.Kind() == reflect.Map
}

// IsSlice indicates that the field value is a slice.
func (attr AttrFieldModel) IsSlice() bool {
	return attr.FieldType.Kind() == reflect.Slice
}

// IsPointer indicates that the field value is a pointer.
func (attr AttrFieldModel) IsPointer() bool {
	return attr.FieldType.Kind() == reflect.Pointer
}

// Analyze builds a struct model for the given type.
//
// TODO if the number of types in a runtime are smallish, we could very reasonably
// provide a global var cache here. It isn't state, AFAICT, it's just a terser
// representation of a reflected analysis.
func Analyze(typ reflect.Type) (model StructModel, err error) {
	if typ.Kind() != reflect.Struct {
		err = NewError("models.notStruct", "type", typ)
		return
	}
	model.Type = typ
	n := typ.NumField()
	attrFields := make(map[Ident]AttrFieldModel, n)
	for i := 0; i < n; i++ {
		fieldType := typ.Field(i)
		ident, attr, fieldErr := parseAttrField(fieldType)
		if fieldErr != nil {
			err = fieldErr
			return
		}
		attr.Index = i
		attrFields[ident] = attr
	}
	model.AttrFields = attrFields
	return
}

func parseAttrField(field reflect.StructField) (ident Ident, attr AttrFieldModel, err error) {
	tag, ok := field.Tag.Lookup("attr")
	if !ok {
		return
	}
	ident, attr, err = parseAttrTag(tag)
	if err != nil {
		return
	}
	attr.FieldType = field.Type
	if ident == sys.DbId {
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
		// This repeats the outer switch, but without the pointer, map or slice cases.
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
			err = NewError("models.invalidPointerType", "tag", tag, "type", field.Type, "kind", field.Type.Elem().Kind())
		}
	default:
		err = NewError("models.invalidType", "tag", tag, "type", field.Type, "kind", field.Type.Kind())
	}
	return
}

func parseAttrTag(tag string) (ident Ident, attr AttrFieldModel, err error) {
	parts := strings.Split(tag, ",")
	ident = Ident(parts[0])
	n := len(parts)
	for i := 1; i < n; i++ {
		part := parts[i]
		switch part {
		case "identity":
			if attr.Unique != 0 {
				err = NewError("models.duplicateUniqueDirective", "tag", tag)
				return
			}
			attr.Unique = sys.AttrUniqueIdentity
		case "unique":
			if attr.Unique != 0 {
				err = NewError("models.duplicateUniqueDirective", "tag", tag)
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
				err = NewError("models.invalidDirective", "tag", tag)
				return
			}
		}
	}
	return
}
