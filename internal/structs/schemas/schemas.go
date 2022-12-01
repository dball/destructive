// Package schemas provides for asserting schemas for structs.
package schemas

import (
	"reflect"
	"strconv"

	"github.com/dball/destructive/internal/structs/models"
	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

func Analyze(typ reflect.Type) (claims []Claim, err error) {
	done := map[reflect.Type]Void{typ: {}}
	todo := map[reflect.Type]Void{typ: {}}
	var nextID uint64 = 1
	todo[typ] = Void{}
	for len(todo) > 0 {
		for typ := range todo {
			model, modelErr := models.Analyze(typ)
			if modelErr != nil {
				err = modelErr
				return
			}
			typeClaims := make([]Claim, 0, 3*len(model.AttrFields))
			for _, field := range model.AttrFields {
				if field.Ident == Ident("sys/db/id") {
					continue
				}
				e := TempID(strconv.FormatUint(uint64(nextID), 10))
				nextID++
				typeClaims = append(typeClaims,
					Claim{E: e, A: sys.DbIdent, V: String(field.Ident)},
					Claim{E: e, A: sys.AttrType, V: field.Type},
				)
				if field.Unique != 0 {
					typeClaims = append(typeClaims, Claim{E: e, A: sys.AttrUnique, V: field.Unique})
				}
				if field.IsMap() || field.IsSlice() {
					typeClaims = append(typeClaims, Claim{E: e, A: sys.AttrCardinality, V: sys.AttrCardinalityMany})
				}
				// TODO if it's a ref field, analyze the ref type
			}
			claims = append(claims, typeClaims...)
			done[typ] = Void{}
			delete(todo, typ)
		}
	}
	return
}
