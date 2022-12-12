package database

import (
	"github.com/dball/destructive/internal/index"
	"github.com/dball/destructive/internal/iterator"
	. "github.com/dball/destructive/internal/types"
)

type indexSnapshot struct {
	eav    index.Index
	aev    index.Index
	ave    index.Index
	vae    index.Index
	idents map[Ident]ID
	attrs  map[ID]Attr
}

var _ Snapshot = (*indexSnapshot)(nil)

func (snapshot *indexSnapshot) Select(claim Claim) (datums iterator.Iterator[Datum]) {
	panic("TODO")
}

func (snapshot *indexSnapshot) Find(claim Claim) (match Datum, found bool) {
	switch e := claim.E.(type) {
	case ID:
		match.E = e
	case Ident:
		match.E = snapshot.idents[e]
	case LookupRef:
		match.E = snapshot.resolveLookupRef(e)
	}
	switch a := claim.A.(type) {
	case ID:
		match.A = a
	case Ident:
		match.A = snapshot.idents[a]
	}
	value, ok := claim.V.(Value)
	if ok {
		match.V = value
	}
	// TODO Find needs to return the datum or at least the t
	found = snapshot.eav.Find(match)
	return
}

func (snapshot *indexSnapshot) resolveLookupRef(ref LookupRef) (id ID) {
	datum := Datum{V: ref.V}
	switch a := ref.A.(type) {
	case ID:
		datum.A = a
	case Ident:
		datum.A = snapshot.idents[a]
	default:
		return
	}
	match, ok := snapshot.ave.First(index.AV, datum)
	if ok {
		id = match.E
	}
	return
}
