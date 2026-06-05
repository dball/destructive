package shredder

import (
	"reflect"
	"time"

	. "github.com/dball/destructive/internal/types"
)

type values []any

// scalarValue converts a scalar reflect.Value to its system Value, reporting whether
// the kind was a recognized scalar. Non-scalar kinds (ref structs, pointers, slices,
// maps) return (nil, false) for the caller to handle.
func scalarValue(v reflect.Value) (Value, bool) {
	switch v.Kind() {
	case reflect.Bool:
		return Bool(v.Bool()), true
	case reflect.Int:
		return Int(v.Int()), true
	case reflect.String:
		return String(v.String()), true
	case reflect.Float64:
		return Float(v.Float()), true
	case reflect.Struct:
		if t, ok := v.Interface().(time.Time); ok {
			return Inst(t), true
		}
	}
	return nil, false
}

// elementValue converts a collection element: scalars become their system Value;
// everything else (struct or pointer refs) is returned raw for ref resolution.
func elementValue(v reflect.Value) any {
	if sv, ok := scalarValue(v); ok {
		return sv
	}
	return v.Interface()
}

func getFieldValue(pointers map[reflect.Value]TempID, fieldType reflect.Type, fieldValue reflect.Value) (val any, err error) {
	switch fieldType.Kind() {
	case reflect.Bool, reflect.Int, reflect.String, reflect.Float64:
		val, _ = scalarValue(fieldValue)
	case reflect.Struct:
		if sv, ok := scalarValue(fieldValue); ok {
			val = sv
		} else {
			val = fieldValue.Interface()
		}
	case reflect.Map:
		var vals values
		iter := fieldValue.MapRange()
		for iter.Next() {
			// TODO we're ignoring the key value on the assumptions that
			// a. the values are structs
			// b. the field appears therein
			// c. the key and struct field value agree
			// these may not obtain, revisit after we add more cardinality many field values
			vals = append(vals, elementValue(iter.Value()))
		}
		val = vals
	case reflect.Slice:
		var vals values
		n := fieldValue.Len()
		for i := range n {
			vals = append(vals, elementValue(fieldValue.Index(i)))
		}
		val = vals
	case reflect.Pointer:
		if fieldValue.IsNil() {
			return
		}
		elem := fieldValue.Elem()
		if sv, ok := scalarValue(elem); ok {
			val = sv
			return
		}
		// A pointer to a ref struct resolves through the pointers map for cycle detection.
		ptr := elem.Addr()
		if tempid, ok := pointers[ptr]; ok {
			val = tempid
		} else {
			val = ptr.Interface()
		}
	default:
		err = NewError("shredder.invalidFieldType", "type", fieldType)
	}
	return
}
