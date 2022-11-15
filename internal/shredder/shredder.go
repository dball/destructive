// Package shredder deconstructs structs into claims.
package shredder

import (
	"reflect"
	"strconv"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

// Shredder shreds structs into claims.
type Shredder interface {
	Retract(x any) (req Request, err error)
	Assert(x any) (req Request, err error)
}

// shredder is a stateful shredder.
type shredder struct {
	nextID uint64
}

func (s *shredder) nextTempID() TempID {
	id := s.nextID
	s.nextID++
	return TempID(strconv.FormatUint(uint64(id), 10))
}

func (s *shredder) Assert(x any) (req Request, err error) {
	e := s.nextTempID()
	tempidConstraints := map[IDRef]Void{}
	req.TempIDs = map[TempID]map[IDRef]Void{e: tempidConstraints}
	typ := reflect.TypeOf(x)
	if typ.Kind() != reflect.Struct {
		err = NewError("shredder.invalidStruct", "type", typ)
		return
	}
	n := typ.NumField()
	req.Claims = make([]*Claim, 0, n)
	for i := 0; i < n; i++ {
		fieldType := typ.Field(i)
		attr, attrErr := parseAttrField(fieldType)
		if attrErr != nil {
			err = attrErr
			return
		}
		if attr.ident == "" {
			continue
		}
		fieldValue := reflect.ValueOf(x).Field(i)
		if attr.ident == sys.DbId {
			switch fieldType.Type.Kind() {
			case reflect.Uint:
				if fieldValue.IsZero() {
					continue
				}
				tempidConstraints[ID(fieldValue.Uint())] = Void{}
			default:
				err = NewError("shredder.invalidIdFieldType", "type", fieldType)
				return
			}
			continue
		}
		vref, fieldErr := getFieldValue(fieldType, fieldValue)
		if fieldErr != nil {
			err = fieldErr
			return
		}
		if vref == nil {
			continue
		}
		v, ok := vref.(Value)
		if !ok {
			err = NewError("shredder.invalidValue", "value", vref)
			return
		}
		if attr.ignoreEmpty && v.IsEmpty() {
			continue
		}
		if attr.unique != 0 {
			tempidConstraints[LookupRef{A: attr.ident, V: v}] = Void{}
		}
		req.Claims = append(req.Claims, &Claim{E: e, A: attr.ident, V: vref})
	}
	return
}

func (s *shredder) Retract(x any) (req Request, err error) {
	e := s.nextTempID()
	req.Claims = []*Claim{{E: e, Retract: true}}
	tempidConstraints := map[IDRef]Void{}
	req.TempIDs = map[TempID]map[IDRef]Void{e: tempidConstraints}
	typ := reflect.TypeOf(x)
	if typ.Kind() != reflect.Struct {
		err = NewError("shredder.invalidStruct", "type", typ)
		return
	}
	n := typ.NumField()
	for i := 0; i < n; i++ {
		fieldType := typ.Field(i)
		attr, attrErr := parseAttrField(fieldType)
		if attrErr != nil {
			err = attrErr
			return
		}
		if attr.ident == "" {
			continue
		}
		fieldValue := reflect.ValueOf(x).Field(i)
		if attr.ident == sys.DbId {
			switch fieldType.Type.Kind() {
			case reflect.Uint:
				if fieldValue.IsZero() {
					continue
				}
				tempidConstraints[ID(fieldValue.Uint())] = Void{}
			default:
				err = NewError("shredder.invalidIdFieldType", "type", fieldType)
				return
			}
			continue
		}
		if attr.unique == 0 {
			continue
		}
		vref, fieldErr := getFieldValue(fieldType, fieldValue)
		if fieldErr != nil {
			err = fieldErr
			return
		}
		if vref == nil {
			continue
		}
		v, ok := vref.(Value)
		if !ok {
			err = NewError("shredder.invalidValue", "value", vref)
			return
		}
		if attr.ignoreEmpty && v.IsEmpty() {
			continue
		}
		tempidConstraints[LookupRef{A: attr.ident, V: v}] = Void{}
	}
	if len(tempidConstraints) == 0 {
		err = NewError("shredder.unidentifiedRetract")
	}
	return
}

// NewShredder returns a new shredder.
func NewShredder() Shredder {
	return &shredder{nextID: uint64(1)}
}
