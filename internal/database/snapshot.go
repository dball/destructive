package database

import (
	"github.com/dball/destructive/internal/index"
	"github.com/dball/destructive/internal/iterator"
	. "github.com/dball/destructive/internal/types"
)

type indexSnapshot struct {
	eav index.Index
	aev index.Index
	ave index.Index
	vae index.Index
}

var _ Snapshot = (*indexSnapshot)(nil)

func (snapshot *indexSnapshot) Select(claim Claim) (datums iterator.Iterator[Datum]) {
	panic("TODO")
}

func (snapshot *indexSnapshot) Find(claim Claim) (match Datum, found bool) {
	switch e := claim.E.(type) {
	case ID:
		match.E = e
	}
	switch a := claim.A.(type) {
	case ID:
		match.A = a
	}
	value, ok := claim.V.(Value)
	if ok {
		match.V = value
	}
	// TODO Find needs to return the datum or at least the t
	found = snapshot.eav.Find(match)
	return
}
