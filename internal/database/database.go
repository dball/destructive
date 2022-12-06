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

	attrsByID    map[ID]Attr
	attrsByIdent map[Ident]Attr
	attrTypes    map[ID]ID
	idents       map[Ident]ID
	uniqueAttrs  map[ID]Void
	refAttrs     map[ID]Void

	lock   sync.RWMutex
	nextID ID
}

var _ Database = (*indexDatabase)(nil)

func NewIndexDatabase(degree int, attrsSize int, identsSize int) (db Database) {
	attrsSize += len(sys.Attrs)
	attrsByID := make(map[ID]Attr, attrsSize)
	attrsByIdent := make(map[Ident]Attr, attrsSize)
	attrTypes := make(map[ID]ID, attrsSize)
	idents := make(map[Ident]ID, identsSize)
	uniqueAttrs := make(map[ID]Void, attrsSize)
	refAttrs := make(map[ID]Void, attrsSize)
	for id, attr := range sys.Attrs {
		attrsByID[id] = attr
		attrsByIdent[attr.Ident] = attr
		attrTypes[id] = attr.Type
		if attr.Unique != 0 {
			uniqueAttrs[id] = Void{}
		}
		if attr.Type == sys.AttrTypeRef {
			refAttrs[id] = Void{}
		}
	}
	for ident, id := range sys.Idents {
		idents[ident] = id
	}
	db = &indexDatabase{
		eav: index.NewCompositeIndex(degree, index.EAVIndex, attrTypes),
		aev: index.NewCompositeIndex(degree, index.AEVIndex, attrTypes),
		ave: index.NewCompositeIndex(degree, index.AVEIndex, attrTypes),
		// TODO vae will only ever need a uint typed index
		vae:          index.NewCompositeIndex(degree, index.VAEIndex, attrTypes),
		attrsByID:    attrsByID,
		attrsByIdent: attrsByIdent,
		attrTypes:    attrTypes,
		idents:       idents,
		uniqueAttrs:  uniqueAttrs,
		nextID:       sys.FirstUserID,
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
	}
	return
}

func (db *indexDatabase) Write(req Request) (res Response) {
	res.NewIDs = map[TempID]ID{}
	db.lock.Lock()
	defer db.lock.Unlock()
	eav := db.eav.Clone()
	aev := db.aev.Clone()
	// TODO could defer this clone until we know we need it
	ave := db.aev.Clone()
	// TODO could defer this clone until we know we need it
	vae := db.vae.Clone()
	t := db.allocateID()
CLAIMS:
	for _, claim := range req.Claims {
		datum := Datum{T: t}
		switch e := claim.E.(type) {
		case ID:
			if e == 0 || e >= t {
				res.Error = NewError("database.write.invalidE", "e", e)
				break CLAIMS
			}
			datum.E = e
		case Ident:
			datum.E = db.idents[e]
			if datum.E == 0 {
				res.Error = NewError("database.write.invalidE", "e", e)
				break CLAIMS
			}
		case LookupRef:
			datum.E = db.resolveLookupRef(e)
			if datum.E == 0 {
				res.Error = NewError("database.write.invalidE", "e", e)
				break CLAIMS
			}
		case TempID:
			datum.E = res.NewIDs[e]
			if datum.E == 0 {
				datum.E = db.allocateID()
				res.NewIDs[e] = datum.E
			}
		case TxnID:
			datum.E = t
		default:
			res.Error = NewError("database.write.invalidE", "e", e)
			break CLAIMS
		}
		switch a := claim.A.(type) {
		case ID:
			if a == 0 || a >= t {
				res.Error = NewError("database.write.invalidA", "a", a)
				break CLAIMS
			}
			datum.A = a
		case Ident:
			datum.A = db.idents[a]
			if datum.A == 0 {
				res.Error = NewError("database.write.invalidA", "a", a)
				break CLAIMS
			}
		case LookupRef:
			datum.A = db.resolveLookupRef(a)
			if datum.A == 0 {
				res.Error = NewError("database.write.invalidA", "a", a)
				break CLAIMS
			}
		default:
			res.Error = NewError("database.write.invalidA", "a", a)
			break CLAIMS
		}
		switch v := claim.V.(type) {
		case Ident:
			found := false
			datum.V, found = db.idents[v]
			if !found {
				res.Error = NewError("database.write.invalidV", "v", v)
				break CLAIMS
			}
		case TempID:
			id := res.NewIDs[v]
			if id == 0 {
				// TODO is it okay if there are no claim e's that correspond to this?
				id = db.allocateID()
				res.NewIDs[v] = id
			}
			datum.V = id
		case LookupRef:
			id := db.resolveLookupRef(v)
			if id == 0 {
				res.Error = NewError("database.write.invalidV", "v", v)
				break CLAIMS
			}
			datum.V = id
		default:
			ok := false
			// TODO we could make ourselves a typed datum right here if we want to commit to that
			// instead of the composite index abstraction, avoiding an intermediate struct thereby.
			datum.V, ok = v.(Value)
			if !ok {
				res.Error = NewError("database.write.invalidV", "v", v)
				break CLAIMS
			}
		}
		if !sys.ValidValue(db.attrTypes[datum.A], datum.V) {
			res.Error = NewError("database.write.inconsistentAV", "datum", datum)
			break CLAIMS
		}
		// TODO we could transact datums into the indexes concurrently after we have resolved all claims
		if !claim.Retract {
			// TODO if this is cardinality one, we must replace extant datum if ea but not v
			eav.Insert(datum)
			aev.Insert(datum)
			ok := false
			_, ok = db.refAttrs[datum.A]
			if ok {
				ave.Insert(datum)
				vae.Insert(datum)
			} else {
				_, ok = db.uniqueAttrs[datum.A]
				if ok {
					ave.Insert(datum)
				}
			}
		} else {
			eav.Delete(datum)
			aev.Delete(datum)
			ok := false
			_, ok = db.refAttrs[datum.A]
			if ok {
				ave.Delete(datum)
				vae.Delete(datum)
			} else {
				_, ok = db.uniqueAttrs[datum.A]
				if ok {
					ave.Delete(datum)
				}
			}
		}
	}
	if res.Error != nil {
		db.nextID = res.ID
		res.NewIDs = nil
	} else {
		res.ID = t
		db.eav = eav
		db.aev = aev
		db.ave = ave
		db.vae = vae
	}
	res.Snapshot = db.read()
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
}

var _ Snapshot = (*indexSnapshot)(nil)

func (snapshot *indexSnapshot) Select(claim Claim) (datums iterator.Iterator[Datum]) {
	panic("TODO")
}
