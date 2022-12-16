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

func (snapshot *indexSnapshot) Select(claim Claim) (datums *iterator.Iterator[Datum]) {
	match := snapshot.resolveClaim(&claim)
	hasE := match.E != 0
	hasA := match.A != 0
	hasV := match.V != nil
	switch hasE {
	case true:
		switch hasA {
		case true:
			switch hasV {
			case true:
				found := snapshot.eav.Find(*match)
				if found {
					panic("TODO single datum iterator")
				}
				panic("TODO empty iterator")
			case false:
				datums = snapshot.eav.Select(index.EA, *match)
			}
		case false:
			switch hasV {
			case true:
				panic("TODO ev? wtd even maybe select all and filter but should we say it's not indexed?")
			case false:
				datums = snapshot.eav.Select(index.E, *match)
			}
		}
	case false:
		switch hasA {
		case true:
			switch hasV {
			case true:
				// TODO validate A is indexed
				datums = snapshot.ave.Select(index.AV, *match)
			case false:
				datums = snapshot.aev.Select(index.A, *match)
			}
		case false:
			switch hasV {
			case true:
				panic("TODO v?? vae is only for back refs anyhow")
			case false:
				panic("TODO maybe just all datums from eav?")
			}
		}
	}
	return
}

func (snapshot *indexSnapshot) Find(claim Claim) (match Datum, found bool) {
	// TODO Find needs to return the datum or at least the t
	found = snapshot.eav.Find(*snapshot.resolveClaim(&claim))
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

func (snapshot *indexSnapshot) resolveClaim(claim *Claim) (match *Datum) {
	match = &Datum{}
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
	return
}
