package shredder

import (
	"reflect"
	"time"

	. "github.com/dball/destructive/internal/types"
)

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
			// TODO recurse, but that probably means we need to pass along the req?
		}
	case reflect.Float64:
		val = Float(fieldValue.Float())
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
					tempid, ok := pointers[fieldValue.Elem().Addr()]

					if ok {
						val = tempid
					} else {
						val = fieldValue.Elem().Addr().Interface()
					}
				}
			}
		}
	default:
		err = NewError("shredder.invalidFieldType", "type", fieldType)
	}
	return
}
