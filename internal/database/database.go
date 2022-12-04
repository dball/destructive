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

	attrsByID    map[ID]Attr
	attrsByIdent map[Ident]Attr
	attrTypes    map[ID]ID

	lock   sync.RWMutex
	nextID ID
}

var _ Database = (*indexDatabase)(nil)

func NewIndexDatabase(degree int, attrsSize int) (db Database) {
	attrsSize += len(sys.Attrs)
	attrsByID := make(map[ID]Attr, attrsSize)
	attrsByIdent := make(map[Ident]Attr, attrsSize)
	attrTypes := make(map[ID]ID, attrsSize)
	for id, attr := range sys.Attrs {
		attrsByID[id] = attr
		attrsByIdent[attr.Ident] = attr
		attrTypes[id] = attr.Type
	}
	db = &indexDatabase{
		eav:          index.NewCompositeIndex(degree, index.EAVIndex, attrTypes),
		attrsByID:    attrsByID,
		attrsByIdent: attrsByIdent,
		attrTypes:    attrTypes,
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
	t := db.allocateID()
CLAIMS:
	for _, claim := range req.Claims {
		datum := Datum{T: t}
		switch e := claim.E.(type) {
		case ID:
			if e == 0 || e > t {
				res.Error = NewError("database.write.invalidE", "e", e)
				break CLAIMS
			}
			datum.E = e
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
		default:
			res.Error = NewError("database.write.invalidA", "a", a)
			break CLAIMS
		}
		switch v := claim.V.(type) {
		case String:
			datum.V = v
		default:
			res.Error = NewError("database.write.invalidV", "v", v)
			break CLAIMS
		}
		eav.Insert(datum)
	}
	if res.Error != nil {
		db.nextID = res.ID
		res.NewIDs = nil
	} else {
		res.ID = t
		db.eav = eav
	}
	res.Snapshot = db.read()
	return
}

func (db *indexDatabase) allocateID() (id ID) {
	id = db.nextID
	db.nextID++
	return
}

type indexSnapshot struct {
	eav index.Index
}

var _ Snapshot = (*indexSnapshot)(nil)

func (snapshot *indexSnapshot) Select(claim Claim) (datums iterator.Iterator[Datum]) {
	panic("TODO")
}
