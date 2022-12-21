// Package shredder deconstructs structs into claims.
package shredder

import (
	"reflect"
	"strconv"

	"github.com/dball/destructive/internal/structs/models"
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
	nextID   uint64
	analyzer models.Analyzer
}

// NewShredder returns a new shredder.
func NewShredder(analyzer models.Analyzer) Shredder {
	return &shredder{nextID: uint64(1), analyzer: analyzer}
}

// confetti are the (internal) results of shredding a document. Note that confetti may
// be inconsistent or invalid.
type confetti struct {
	// pointers associate pointers to shredded structs with their tempids in the request
	pointers map[reflect.Value]TempID
	// tempIDs are the registry of temp ids allocated by shredding the document.
	tempIDs map[TempID]ID
}

func (s *shredder) nextTempID() TempID {
	id := s.nextID
	s.nextID++
	return TempID(strconv.FormatUint(uint64(id), 10))
}

func (s *shredder) Shred(doc Document) (req Request, err error) {
	confetti := confetti{
		tempIDs:  make(map[TempID]ID, len(doc.Assertions)),
		pointers: make(map[reflect.Value]TempID, len(doc.Assertions)),
	}
	// The likely size here is actually assertions*numFields
	req.Claims = make([]*Claim, 0, len(doc.Assertions))
	req.Retractions = make([]*Retraction, 0, len(doc.Retractions))
	for _, x := range doc.Retractions {
		var retraction *Retraction
		retraction, err = s.retract(&confetti, x)
		if err != nil {
			return
		}
		req.Retractions = append(req.Retractions, retraction)
	}
	for _, x := range doc.Assertions {
		var claims []*Claim
		_, claims, err = s.assert(&confetti, x)
		if err != nil {
			return
		}
		req.Claims = append(req.Claims, claims...)
	}
	for _, claim := range req.Claims {
		e, ok := claim.E.(TempID)
		if ok {
			id := confetti.tempIDs[e]
			if id != 0 {
				claim.E = id
			}
		}
		v, ok := claim.V.(TempID)
		if ok {
			id := confetti.tempIDs[v]
			if id != 0 {
				claim.V = id
			}
		}
	}
	return
}

func (s *shredder) assert(confetti *confetti, x any) (e TempID, claims []*Claim, err error) {
	typ := reflect.TypeOf(x)
	var fields reflect.Value
	var id ID
	switch typ.Kind() {
	case reflect.Struct:
		fields = reflect.ValueOf(x)
		e = s.nextTempID()
	case reflect.Pointer:
		ptr := reflect.ValueOf(x)
		if ptr.IsNil() {
			err = NewError("shredder.nilStruct")
			return
		}
		_, ok := confetti.pointers[ptr]
		if ok {
			return
		}
		e = s.nextTempID()
		confetti.pointers[ptr] = e
		fields = ptr.Elem()
		typ = fields.Type()
	default:
		err = NewError("shredder.invalidStruct", "type", typ)
		return
	}
	model, modelErr := s.analyzer.Analyze(typ)
	if modelErr != nil {
		err = modelErr
		return
	}
	claims = make([]*Claim, 0, len(model.AttrFields))
	var refFieldsClaims []*Claim
	for _, attr := range model.AttrFields {
		fieldValue := fields.Field(attr.Index)
		if attr.Ident == sys.DbId {
			switch attr.FieldType.Kind() {
			case reflect.Uint:
				fid := ID(fieldValue.Uint())
				switch {
				case id == 0:
					id = fid
					confetti.tempIDs[e] = id
				case id != fid:
					err = NewError("shredder.inconsistentEs", "id1", id, "id2", fid)
					return
				}
			default:
				err = NewError("shredder.invalidIdFieldType", "type", attr.FieldType)
				return
			}
			continue
		}
		val, fieldErr := getFieldValue(confetti.pointers, attr.FieldType, fieldValue)
		if fieldErr != nil {
			err = fieldErr
			return
		}
		if val == nil {
			continue
		}
		var vref VRef
		switch v := val.(type) {
		case Value:
			vref = v.(VRef)
			if attr.IgnoreEmpty && v.IsEmpty() {
				continue
			}
		case TempID:
			vref = v
			// TODO idk if tempid constraints are legit or not
		case values:
			for i, vv := range v {
				var refFieldClaims []*Claim
				if attr.CollValue != "" {
					vvv, ok := ToVRef(vv)
					if !ok {
						err = NewError("shredder.invalidSliceValue")
						return
					}
					ve := s.nextTempID()
					refFieldsClaims = append(refFieldsClaims,
						&Claim{E: ve, A: Ident("sys/db/rank"), V: Int(i)},
						&Claim{E: ve, A: attr.CollValue, V: vvv},
					)
					claims = append(claims, &Claim{E: e, A: attr.Ident, V: ve})
				} else {
					vref, refFieldClaims, err = s.assert(confetti, vv)
					if err != nil {
						return
					}
					switch {
					case attr.MapKey != "":
						refFieldsClaims = append(refFieldsClaims, refFieldClaims...)
					case len(refFieldClaims) > 0:
						refFieldsClaims = append(refFieldsClaims, &Claim{E: refFieldClaims[0].E, A: Ident("sys/db/rank"), V: Int(i)})
						refFieldsClaims = append(refFieldsClaims, refFieldClaims...)
					default:
						err = NewError("shredder.missingSliceCollectionValue")
						return
					}
					claims = append(claims, &Claim{E: e, A: attr.Ident, V: vref})
				}
			}
			continue
		default:
			var refFieldClaims []*Claim
			vref, refFieldClaims, err = s.assert(confetti, v)
			if err != nil {
				return
			}
			refFieldsClaims = append(refFieldsClaims, refFieldClaims...)
		}
		claims = append(claims, &Claim{E: e, A: attr.Ident, V: vref})
	}
	claims = append(claims, refFieldsClaims...)
	return
}

func (s *shredder) retract(confetti *confetti, x any) (retraction *Retraction, err error) {
	constraints := map[IDRef]Void{}
	var fields reflect.Value
	typ := reflect.TypeOf(x)
	var e ID
	switch typ.Kind() {
	case reflect.Struct:
		fields = reflect.ValueOf(x)
	case reflect.Pointer:
		ptr := reflect.ValueOf(x)
		if ptr.IsNil() {
			err = NewError("shredder.nilStruct")
			return
		}
		fields = ptr.Elem()
		typ = fields.Type()
	default:
		err = NewError("shredder.invalidStruct", "type", typ)
		return
	}
	model, modelErr := s.analyzer.Analyze(typ)
	if modelErr != nil {
		err = modelErr
		return
	}
	for _, attr := range model.AttrFields {
		fieldValue := fields.Field(attr.Index)
		if attr.Ident == sys.DbId {
			switch attr.FieldType.Kind() {
			case reflect.Uint:
				if fieldValue.IsZero() {
					continue
				}
				id := ID(fieldValue.Uint())
				switch {
				case e == 0:
					e = id
					constraints[e] = Void{}
				case e != id:
					err = NewError("shredder.inconsistentEs", "id1", e, "id2", id)
					return
				}
				constraints[ID(fieldValue.Uint())] = Void{}
			default:
				err = NewError("shredder.invalidIdFieldType", "type", attr.FieldType)
				return
			}
			continue
		}
		if attr.Unique == 0 {
			continue
		}
		vref, fieldErr := getFieldValue(confetti.pointers, attr.FieldType, fieldValue)
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
		if attr.IgnoreEmpty && v.IsEmpty() {
			continue
		}
		constraints[LookupRef{A: attr.Ident, V: v}] = Void{}
	}
	if len(constraints) == 0 {
		err = NewError("shredder.unidentifiedRetract")
		return
	}
	retraction = &Retraction{Constraints: constraints}
	return
}
