// Package shredder deconstructs structs into claims.
package shredder

import (
	"reflect"
	"strconv"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

// Document contains the lists of structs to assert or retract.
type Document struct {
	Retractions []any
	Assertions  []any
}

// Shredder shreds structs into claims.
type Shredder interface {
	Shred(doc Document) (req Request, err error)
}

// shredder is a stateful shredder.
type shredder struct {
	nextID uint64
}

// NewShredder returns a new shredder.
func NewShredder() Shredder {
	return &shredder{nextID: uint64(1)}
}

func (s *shredder) nextTempID() TempID {
	id := s.nextID
	s.nextID++
	return TempID(strconv.FormatUint(uint64(id), 10))
}

func (s *shredder) Shred(doc Document) (req Request, err error) {
	total := len(doc.Assertions) + len(doc.Retractions)
	req.TempIDs = make(map[TempID]map[IDRef]Void, total)
	// The likely size here is actually assertions*numFields + retractions
	req.Claims = make([]*Claim, 0, total)
	pointers := make(map[reflect.Value]TempID, total)
	for _, x := range doc.Retractions {
		err = s.retract(&req, pointers, x)
		if err != nil {
			return
		}
	}
	for _, x := range doc.Assertions {
		err = s.assert(&req, pointers, x)
		if err != nil {
			return
		}
	}
	return
}

func (s *shredder) assert(req *Request, pointers map[reflect.Value]TempID, x any) (err error) {
	e := s.nextTempID()
	tempidConstraints := map[IDRef]Void{}
	req.TempIDs[e] = tempidConstraints
	typ := reflect.TypeOf(x)
	var fields reflect.Value
	switch typ.Kind() {
	case reflect.Struct:
		fields = reflect.ValueOf(x)
	case reflect.Pointer:
		ptr := reflect.ValueOf(x)
		if ptr.IsNil() {
			err = NewError("shredder.nilStruct")
			return
		}
		pointers[ptr] = e
		fields = ptr.Elem()
		typ = fields.Type()
	default:
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
		fieldValue := fields.Field(i)
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
		vref, fieldErr := getFieldValue(pointers, fieldType, fieldValue)
		if fieldErr != nil {
			err = fieldErr
			return
		}
		if vref == nil {
			continue
		}
		v, ok := vref.(Value)
		if ok {
			if attr.ignoreEmpty && v.IsEmpty() {
				continue
			}
			if attr.unique != 0 {
				tempidConstraints[LookupRef{A: attr.ident, V: v}] = Void{}
			}
		}
		req.Claims = append(req.Claims, &Claim{E: e, A: attr.ident, V: vref})
	}
	return
}

func (s *shredder) retract(req *Request, pointers map[reflect.Value]TempID, x any) (err error) {
	e := s.nextTempID()
	req.Claims = append(req.Claims, &Claim{E: e, Retract: true})
	tempidConstraints := map[IDRef]Void{}
	req.TempIDs[e] = tempidConstraints
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
		vref, fieldErr := getFieldValue(pointers, fieldType, fieldValue)
		if fieldErr != nil {
			err = fieldErr
			return
		}
		if vref == nil {
			continue
		}
		v, ok := vref.(Value)
		if !ok {
			continue
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
