package shredder

import (
	"reflect"
	"time"

	. "github.com/dball/destructive/internal/types"
)

type values []any

func getFieldValue(pointers map[reflect.Value]TempID, fieldType reflect.StructField, fieldValue reflect.Value) (val any, err error) {
	switch fieldType.Type.Kind() {
	case reflect.Bool:
		val = Bool(fieldValue.Bool())
	case reflect.Int:
		val = Int(fieldValue.Int())
	case reflect.String:
		val = String(fieldValue.String())
	case reflect.Struct:
		v := fieldValue.Interface()
		switch typed := v.(type) {
		case time.Time:
			val = Inst(typed)
		default:
			val = fieldValue.Interface()
		}
	case reflect.Float64:
		val = Float(fieldValue.Float())
	case reflect.Map:
		var vals values
		iter := fieldValue.MapRange()
		for iter.Next() {
			// TODO we're ignoring the key value on the assumptions that
			// a. the values are structs
			// b. the field appears therein
			// c. the key and struct field value agree
			// these may not obtain, revisit after we add more cardinality many field values
			vals = append(vals, iter.Value().Interface())
		}
		val = vals
	case reflect.Slice:
		var vals values
		n := fieldValue.Len()
		for i := 0; i < n; i++ {
			vals = append(vals, fieldValue.Index(i).Interface())
		}
		val = vals
	case reflect.Pointer:
		if !fieldValue.IsNil() {
			switch fieldType.Type.Elem().Kind() {
			case reflect.Bool:
				val = Bool(fieldValue.Elem().Bool())
			case reflect.Int:
				val = Int(fieldValue.Elem().Int())
			case reflect.String:
				val = String(fieldValue.Elem().String())
			case reflect.Struct:
				v := fieldValue.Elem().Interface()
				switch typed := v.(type) {
				case time.Time:
					val = Inst(typed)
				default:
					ptr := fieldValue.Elem().Addr()
					tempid, ok := pointers[ptr]
					if ok {
						val = tempid
					} else {
						val = ptr.Interface()
					}
				}
			}
		}
	default:
		err = NewError("shredder.invalidFieldType", "type", fieldType)
	}
	return
}
