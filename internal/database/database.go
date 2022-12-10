// Package database contains the primary database implementation.
package database

import (
	"sync"

	"github.com/dball/destructive/internal/index"
	"github.com/dball/destructive/internal/iterator"
	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

type indexDatabase struct {
	eav index.Index
	aev index.Index
	ave index.Index
	vae index.Index

	attrsByID      map[ID]Attr
	attrsByIdent   map[Ident]Attr
	attrTypes      map[ID]ID
	attrUniques    map[ID]ID
	attrCardManies map[ID]Void
	idents         map[Ident]ID

	lock   sync.RWMutex
	nextID ID
}

var _ Database = (*indexDatabase)(nil)

func NewIndexDatabase(degree int, attrsSize int, identsSize int) (db Database) {
	attrsSize += len(sys.Attrs)
	attrsByID := make(map[ID]Attr, attrsSize)
	attrsByIdent := make(map[Ident]Attr, attrsSize)
	attrTypes := make(map[ID]ID, attrsSize)
	attrUniques := make(map[ID]ID, attrsSize)
	attrCardManies := make(map[ID]Void, attrsSize)
	idents := make(map[Ident]ID, identsSize)
	for id, attr := range sys.Attrs {
		attrsByID[id] = attr
		attrsByIdent[attr.Ident] = attr
		attrTypes[id] = attr.Type
		if attr.Unique != 0 {
			attrUniques[id] = attr.Unique
		}
		if attr.Cardinality == sys.AttrCardinalityMany {
			attrCardManies[id] = Void{}
		}
	}
	for ident, id := range sys.Idents {
		idents[ident] = id
	}
	eav := index.NewCompositeIndex(degree, index.EAVIndex, attrTypes)
	aev := index.NewCompositeIndex(degree, index.AEVIndex, attrTypes)
	ave := index.NewCompositeIndex(degree, index.AVEIndex, attrTypes)
	// TODO vae will only ever need a uint typed index
	vae := index.NewCompositeIndex(degree, index.VAEIndex, attrTypes)
	// Bootstrap the system datums by writing to the appropriate indexes directly.
	for _, datum := range sys.Datums {
		eav.Insert(datum)
		aev.Insert(datum)
		_, ok := attrUniques[datum.A]
		if ok {
			ave.Insert(datum)
		}
		if attrTypes[datum.A] == sys.AttrTypeRef {
			vae.Insert(datum)
		}
	}
	db = &indexDatabase{
		eav:            eav,
		aev:            aev,
		ave:            ave,
		vae:            vae,
		attrsByID:      attrsByID,
		attrsByIdent:   attrsByIdent,
		attrTypes:      attrTypes,
		attrUniques:    attrUniques,
		attrCardManies: attrCardManies,
		idents:         idents,
		nextID:         sys.FirstUserID,
	}
	return
}

func (db *indexDatabase) Read() (snapshot Snapshot) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	snapshot = db.read()
	return
}

func (db *indexDatabase) read() (snapshot Snapshot) {
	snapshot = &indexSnapshot{
		eav: db.eav.Clone(),
		aev: db.aev.Clone(),
		ave: db.ave.Clone(),
		vae: db.vae.Clone(),
		// TODO idents and attrs, probably
	}
	return
}

func (db *indexDatabase) Write(req Request) (res Response) {
	res.NewIDs = map[TempID]ID{}
	assigned := map[ID]TempID{}
	rewrites := map[ID]ID{}
	db.lock.Lock()
	defer db.lock.Unlock()
	lastID := db.nextID
	res.ID = db.allocateID()
	data := make([]*Datum, 0, len(req.Claims))
CLAIMS:
	for _, claim := range req.Claims {
		datum := db.evaluateClaim(&res, assigned, claim)
		if res.Error != nil {
			break
		}
		if !claim.Retract {
			unique := db.attrUniques[datum.A]
			if unique != 0 {
				// TODO First is not returning the value we expect here :(
				// db.ave does not appear to be sorted as expected!!
				d, ok := db.ave.First(index.AV, *datum)
				if ok {
					switch unique {
					case sys.AttrUniqueIdentity:
						_, ok := rewrites[datum.E]
						if ok {
							res.Error = NewError("database.write.uniqueValueImpossible", "datum", datum)
							break CLAIMS
						}
						rewrites[datum.E] = d.E
					case sys.AttrUniqueValue:
						res.Error = NewError("database.write.uniqueValueCollision", "datum", datum, "extant", d)
						break CLAIMS
					}
				} else {
					iter := db.ave.Select(index.AV, *datum)
					if iter.Next() {
						panic("WHAT")
					}
				}
			}
		}
		data = append(data, datum)
	}
	// We now have datums with resolved or assigned ids and consistent avs.
	if res.Error == nil {
		eav := db.eav.Clone()
		aev := db.aev.Clone()
		// TODO could defer this clone until we know we need it
		ave := db.aev.Clone()
		// TODO could defer this clone until we know we need it
		vae := db.vae.Clone()
		for i, datum := range data {
			// TODO we could transact datums into the indexes concurrently after we have resolved all claims
			claim := req.Claims[i]
			if !claim.Retract {
				_, ok := db.attrCardManies[datum.A]
				if !ok {
					// if this is cardinality one, we must replace extant datum if ea but not v
					d, ok := eav.First(index.EA, *datum)
					if ok {
						if d.V == datum.V {
							continue
						} else {
							eav.Delete(d)
							aev.Delete(d)
							_, ok := db.attrUniques[datum.A]
							if ok {
								ave.Delete(d)
							}
							if db.attrTypes[datum.A] == sys.AttrTypeRef {
								vae.Delete(d)
							}
						}
					}
				}
				eav.Insert(*datum)
				aev.Insert(*datum)
				_, ok = db.attrUniques[datum.A]
				if ok {
					ave.Insert(*datum)
				}
				if db.attrTypes[datum.A] == sys.AttrTypeRef {
					vae.Insert(*datum)
				}
			} else {
				eav.Delete(*datum)
				aev.Delete(*datum)
				_, ok := db.attrUniques[datum.A]
				if ok {
					ave.Delete(*datum)
				}
				if db.attrTypes[datum.A] == sys.AttrTypeRef {
					vae.Delete(*datum)
				}
			}
		}
		db.eav = eav
		db.aev = aev
		db.ave = ave
		db.vae = vae
	}
	if res.Error != nil {
		res.ID = 0
		db.nextID = lastID
		res.NewIDs = nil
	}
	res.Snapshot = db.read()
	return
}

func (db *indexDatabase) evaluateClaim(res *Response, assigned map[ID]TempID, claim *Claim) (datum *Datum) {
	datum = &Datum{T: res.ID}
	switch e := claim.E.(type) {
	case ID:
		if e == 0 || e >= res.ID {
			res.Error = NewError("database.write.invalidE", "e", e)
		}
		datum.E = e
	case Ident:
		datum.E = db.idents[e]
		if datum.E == 0 {
			res.Error = NewError("database.write.invalidE", "e", e)
		}
	case LookupRef:
		datum.E = db.resolveLookupRef(e)
		if datum.E == 0 {
			res.Error = NewError("database.write.invalidE", "e", e)
		}
	case TempID:
		datum.E = res.NewIDs[e]
		if datum.E == 0 {
			datum.E = db.allocateID()
			res.NewIDs[e] = datum.E
			assigned[datum.E] = e
		}
	case TxnID:
		datum.E = res.ID
	default:
		res.Error = NewError("database.write.invalidE", "e", e)
	}
	if res.Error != nil {
		return
	}
	switch a := claim.A.(type) {
	case ID:
		if a == 0 || a >= res.ID {
			res.Error = NewError("database.write.invalidA", "a", a)
		}
		datum.A = a
	case Ident:
		datum.A = db.idents[a]
		if datum.A == 0 {
			res.Error = NewError("database.write.invalidA", "a", a)
		}
	case LookupRef:
		datum.A = db.resolveLookupRef(a)
		if datum.A == 0 {
			res.Error = NewError("database.write.invalidA", "a", a)
		}
	default:
		res.Error = NewError("database.write.invalidA", "a", a)
	}
	if res.Error != nil {
		return
	}
	switch v := claim.V.(type) {
	case Ident:
		found := false
		datum.V, found = db.idents[v]
		if !found {
			res.Error = NewError("database.write.invalidV", "v", v)
		}
	case TempID:
		id := res.NewIDs[v]
		if id == 0 {
			// TODO is it okay if there are no claim e's that correspond to this?
			id = db.allocateID()
			res.NewIDs[v] = id
			assigned[id] = v
		}
		datum.V = id
	case LookupRef:
		id := db.resolveLookupRef(v)
		if id == 0 {
			res.Error = NewError("database.write.invalidV", "v", v)
		}
		datum.V = id
	default:
		ok := false
		// TODO we could make ourselves a typed datum right here if we want to commit to that
		// instead of the composite index abstraction, avoiding an intermediate struct thereby.
		datum.V, ok = v.(Value)
		if !ok {
			res.Error = NewError("database.write.invalidV", "v", v)
		}
	}
	if res.Error != nil {
		return
	}
	if !sys.ValidValue(db.attrTypes[datum.A], datum.V) {
		res.Error = NewError("database.write.inconsistentAV", "datum", datum)
	}
	return
}

func (db *indexDatabase) allocateID() (id ID) {
	id = db.nextID
	db.nextID++
	return
}

func (db *indexDatabase) resolveLookupRef(ref LookupRef) (id ID) {
	// TODO need ave index
	return
}

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
