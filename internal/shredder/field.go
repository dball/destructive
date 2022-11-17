package shredder

import (
	"reflect"
	"time"

	. "github.com/dball/destructive/internal/types"
)

func getFieldValue(pointers map[reflect.Value]TempID, fieldType reflect.StructField, fieldValue reflect.Value) (vref VRef, err error) {
	switch fieldType.Type.Kind() {
	case reflect.Bool:
		vref = Bool(fieldValue.Bool())
	case reflect.Int:
		vref = Int(fieldValue.Int())
	case reflect.String:
		vref = String(fieldValue.String())
	case reflect.Struct:
		v := fieldValue.Interface()
		switch typed := v.(type) {
		case time.Time:
			vref = Inst(typed)
		default:
			// TODO recurse, but that probably means we need to pass along the req?
		}
	case reflect.Float64:
		vref = Float(fieldValue.Float())
	case reflect.Pointer:
		if !fieldValue.IsNil() {
			switch fieldType.Type.Elem().Kind() {
			case reflect.Bool:
				vref = Bool(fieldValue.Elem().Bool())
			case reflect.Int:
				vref = Int(fieldValue.Elem().Int())
			case reflect.String:
				vref = String(fieldValue.Elem().String())
			case reflect.Struct:
				v := fieldValue.Elem().Interface()
				switch typed := v.(type) {
				case time.Time:
					vref = Inst(typed)
				default:
					tempid, ok := pointers[fieldValue.Elem().Addr()]
					if ok {
						vref = tempid
					} else {
						// TODO recurse, but that probably means we need to pass along the req?
					}
				}
			}
		}
	default:
		err = NewError("shredder.invalidFieldType", "type", fieldType)
	}
	return
}
