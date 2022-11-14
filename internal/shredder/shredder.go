// Package shredder deconstructs structs into claims.
package shredder

import (
	"errors"
	"reflect"
	"strconv"
	"time"

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

func (s *shredder) Retract(x any) (req Request, err error) {
	e := s.nextTempID()
	req.Claims = []*Claim{{E: e, Retract: true}}
	tempidConstraints := map[IDRef]Void{}
	req.TempIDs = map[TempID]map[IDRef]Void{e: tempidConstraints}
	typ := reflect.TypeOf(x)
	if typ.Kind() != reflect.Struct {
		err = errors.New("invalid type")
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
				tempidConstraints[ID(fieldValue.Uint())] = Void{}
			default:
				// TODO error?
			}
			continue
		}
		if attr.unique == 0 {
			continue
		}
		// TODO extract into helper fn
		var vref VRef
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
		default:
			// TODO error?
			continue
		}
		v, ok := vref.(Value)
		if !ok {
			// TODO panic or err or
			continue
		}
		// TODO the isEmpty control bit might be a flag on the struct field
		if !v.IsEmpty() {
			tempidConstraints[LookupRef{A: attr.ident, V: v}] = Void{}
		}
	}
	return
}

func (s *shredder) Assert(x any) (req Request, err error) {
	e := s.nextTempID()
	tempidConstraints := map[IDRef]Void{}
	req.TempIDs = map[TempID]map[IDRef]Void{e: tempidConstraints}
	typ := reflect.TypeOf(x)
	if typ.Kind() != reflect.Struct {
		err = errors.New("invalid type")
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
				tempidConstraints[ID(fieldValue.Uint())] = Void{}
			default:
				// TODO error?
			}
			continue
		}
		// TODO extract into helper fn
		var vref VRef
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
		default:
			// TODO error?
			continue
		}
		v, ok := vref.(Value)
		if !ok {
			// TODO panic or err or
			continue
		}
		// TODO the isEmpty control bit might be a flag on the struct field
		if v.IsEmpty() {
			continue
		}
		if attr.unique != 0 {
			tempidConstraints[LookupRef{A: attr.ident, V: v}] = Void{}
		}
		req.Claims = append(req.Claims, &Claim{E: e, A: attr.ident, V: vref})
	}
	return
}

// NewShredder returns a new shredder.
func NewShredder() Shredder {
	return &shredder{nextID: uint64(1)}
}
