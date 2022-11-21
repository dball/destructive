// Package assembler provides for the construction of structs from sets of datums.
package assembler

import (
	"reflect"

	. "github.com/dball/destructive/internal/types"
)

func Assemble(target any, datums []Datum) (err error) {
	ptr := reflect.ValueOf(target)
	if ptr.Kind() != reflect.Pointer {
		return NewError("assembler.targetNotPointer")
	}
	value := ptr.Elem()
	if value.Kind() != reflect.Struct {
		return NewError("assembler.targetValueNotStruct")
	}
	panic("TODO")
	/*
		typ := ptr.Type()
		n := typ.NumField()
		for i := 0; i < n; i++ {
			field := typ.Field(i)
			attrTag, err := attrs.ParseAttrField(field)
			if err != nil {
				return
			}
			fieldValue := ptr.Field(i).Interface()
		}
		return nil
	*/
}
