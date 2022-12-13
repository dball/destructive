package database

import (
	"strconv"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

// Declare registers the list of attributes in the database.
//
// This is purely a helper function for brevity, callers are free to write claims
// asserting attributes directly.
func Declare(db Database, attrs ...Attr) (err error) {
	err = db.Write(Request{Claims: attrClaims(attrs)}).Error
	return
}

func attrClaims(attrs []Attr) (claims []*Claim) {
	claims = make([]*Claim, 0, len(attrs)*4)
	for i, attr := range attrs {
		e := TempID(strconv.Itoa(i))
		claims = append(claims,
			&Claim{E: e, A: sys.DbIdent, V: String(attr.Ident)},
			&Claim{E: e, A: sys.AttrType, V: attr.Type},
		)
		if attr.Unique != 0 {
			claims = append(claims,
				&Claim{E: e, A: sys.AttrUnique, V: attr.Unique},
			)
		}
		if attr.Cardinality != 0 {
			claims = append(claims,
				&Claim{E: e, A: sys.AttrCardinality, V: attr.Cardinality},
			)
		}
	}
	return
}
