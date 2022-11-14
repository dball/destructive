// Package shredder deconstructs structs into claims.
package shredder

import (
	"reflect"
	"strconv"
	"time"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

// Shredder shreds structs into claims.
type Shredder interface {
	Retract(x any) (req Request, err error)
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
	tempid := s.nextTempID()
	req.Claims = []*Claim{{E: tempid, Retract: true}}
	tempidConstraints := map[IDRef]Void{}
	req.TempIDs = map[TempID]map[IDRef]Void{tempid: tempidConstraints}
	typ := reflect.TypeOf(x)
	// TODO return an error instead of panicking if x is not a struct
	n := typ.NumField()
	for i := 0; i < n; i++ {
		fieldType := typ.Field(i)
		attr := parseAttrField(fieldType)
		if attr.Ident == "" {
			continue
		}
		fieldValue := reflect.ValueOf(x).Field(i)
		if attr.Ident == sys.DbId {
			switch fieldType.Type.Kind() {
			case reflect.Uint:
				tempidConstraints[ID(fieldValue.Uint())] = Void{}
			default:
				// TODO error?
			}
			continue
		} else if attr.Unique != 0 {
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
			// TODO the isEmpty control bit might be a flag on the struct field
			if ok && !v.IsEmpty() {
				tempidConstraints[LookupRef{A: attr.Ident, V: v}] = Void{}
			}
		}
	}
	return
}

// NewShredder returns a new shredder.
func NewShredder() Shredder {
	return &shredder{nextID: uint64(1)}
}
