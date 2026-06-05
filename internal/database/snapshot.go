package database

import (
	"iter"
	"time"

	"github.com/dball/destructive/internal/index"
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

// Select returns the datums matching the claim. A zero field in the resolved claim
// matches any value, so the claim's populated fields choose the access path: the
// best available index is used, and any remaining constraint the index cannot
// express (a value over a non-value-indexed access) is applied as a filter.
func (snapshot *indexSnapshot) Select(claim Claim) (datums iter.Seq[Datum]) {
	match := snapshot.resolveClaim(claim)
	hasE := match.E != 0
	hasA := match.A != 0
	hasV := match.V != nil
	switch {
	case hasE && hasA && hasV:
		datums = filterByV(snapshot.eav.Select(index.EA, match), match.V)
	case hasE && hasA:
		datums = snapshot.eav.Select(index.EA, match)
	case hasE && hasV:
		datums = filterByV(snapshot.eav.Select(index.E, match), match.V)
	case hasE:
		datums = snapshot.eav.Select(index.E, match)
	case hasA && hasV:
		// A unique attribute is indexed by value; otherwise scan the attribute and
		// filter by value.
		if snapshot.attrs[match.A].Unique != 0 {
			datums = snapshot.ave.Select(index.AV, match)
		} else {
			datums = filterByV(snapshot.aev.Select(index.A, match), match.V)
		}
	case hasA:
		datums = snapshot.aev.Select(index.A, match)
	case hasV:
		datums = filterByV(snapshot.eav.All(), match.V)
	default:
		datums = snapshot.eav.All()
	}
	return
}

func (snapshot *indexSnapshot) Find(claim Claim) (match Datum, found bool) {
	// TODO Find needs to return the datum or at least the t
	match = snapshot.resolveClaim(claim)
	found = snapshot.eav.Find(match)
	return
}

// Count returns the number of datums matching the claim. It shares Select's access
// logic; the typed indexes count by ascending matches, the same cost as iterating.
func (snapshot *indexSnapshot) Count(claim Claim) (count int) {
	for range snapshot.Select(claim) {
		count++
	}
	return
}

// filterByV yields only the datums whose value equals v.
func filterByV(seq iter.Seq[Datum], v Value) iter.Seq[Datum] {
	return func(yield func(Datum) bool) {
		for d := range seq {
			if valuesEqual(d.V, v) {
				if !yield(d) {
					return
				}
			}
		}
	}
}

// valuesEqual compares two values with the same instant semantics the indexes use:
// Inst values are equal when they denote the same millisecond, matching how they are
// stored and compared in the int tree. All other value types compare by ==.
func valuesEqual(a Value, b Value) bool {
	ai, aok := a.(Inst)
	bi, bok := b.(Inst)
	if aok && bok {
		return time.Time(ai).UnixMilli() == time.Time(bi).UnixMilli()
	}
	return a == b
}

func (snapshot *indexSnapshot) ResolveIdent(ident Ident) (id ID) {
	id = snapshot.idents[ident]
	return
}

func (snapshot *indexSnapshot) ResolveAttrIdent(id ID) (ident Ident) {
	ident = snapshot.attrs[id].Ident
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
	// Only unique attributes are maintained in the ave index, so a lookup ref against
	// any other attribute cannot resolve to a single entity.
	if snapshot.attrs[datum.A].Unique == 0 {
		return
	}
	match, ok := snapshot.ave.First(index.AV, datum)
	if ok {
		id = match.E
	}
	return
}

func (snapshot *indexSnapshot) resolveClaim(claim Claim) (match Datum) {
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
