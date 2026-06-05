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
	idents map[Ident]ID
	attrs  map[ID]Attr
}

var _ Snapshot = (*indexSnapshot)(nil)

// access describes how to answer a resolved claim: which index and partial to read,
// whether to scan every datum, and an optional value to post-filter by for shapes
// the chosen index cannot constrain on value. Select and Count share it so the
// access logic lives in one place.
type access struct {
	index   index.Index
	partial index.PartialIndex
	all     bool
	filterV Value
}

// access chooses the read path for a resolved claim. A zero field matches any value,
// so the populated fields select the best available index; a value the index cannot
// constrain on is recorded in filterV for the caller to apply.
func (snapshot *indexSnapshot) access(match Datum) access {
	hasE := match.E != 0
	hasA := match.A != 0
	hasV := match.V != nil
	switch {
	case hasE && hasA && hasV:
		return access{index: snapshot.eav, partial: index.EA, filterV: match.V}
	case hasE && hasA:
		return access{index: snapshot.eav, partial: index.EA}
	case hasE && hasV:
		return access{index: snapshot.eav, partial: index.E, filterV: match.V}
	case hasE:
		return access{index: snapshot.eav, partial: index.E}
	case hasA && hasV:
		// A unique attribute is indexed by value; otherwise scan the attribute and
		// filter by value.
		if snapshot.attrs[match.A].Unique != 0 {
			return access{index: snapshot.ave, partial: index.AV}
		}
		return access{index: snapshot.aev, partial: index.A, filterV: match.V}
	case hasA:
		return access{index: snapshot.aev, partial: index.A}
	case hasV:
		return access{all: true, filterV: match.V}
	default:
		return access{all: true}
	}
}

func (snapshot *indexSnapshot) selectWith(a access, match Datum) (datums iter.Seq[Datum]) {
	if a.all {
		datums = snapshot.eav.All()
	} else {
		datums = a.index.Select(a.partial, match)
	}
	if a.filterV != nil {
		datums = filterByV(datums, a.filterV)
	}
	return
}

// Select returns the datums matching the claim.
func (snapshot *indexSnapshot) Select(claim Claim) iter.Seq[Datum] {
	match := snapshot.resolveClaim(claim)
	return snapshot.selectWith(snapshot.access(match), match)
}

// Has reports whether any datum matches the claim. A fully specified claim is an
// exact existence check against eav (a btree lookup, no allocation); other shapes
// fall back to testing whether the access path yields anything.
func (snapshot *indexSnapshot) Has(claim Claim) bool {
	match := snapshot.resolveClaim(claim)
	if match.E != 0 && match.A != 0 && match.V != nil {
		return snapshot.eav.Find(match)
	}
	for range snapshot.selectWith(snapshot.access(match), match) {
		return true
	}
	return false
}

// Count returns the number of datums matching the claim. Indexed shapes count
// directly through the typed indexes (which tally without materializing values);
// value-filtered and unconstrained shapes are counted by enumeration.
func (snapshot *indexSnapshot) Count(claim Claim) (count int) {
	match := snapshot.resolveClaim(claim)
	a := snapshot.access(match)
	if a.all || a.filterV != nil {
		for range snapshot.selectWith(a, match) {
			count++
		}
		return
	}
	return a.index.Count(a.partial, match)
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
