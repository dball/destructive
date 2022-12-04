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

	lock sync.RWMutex
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
	}
	return
}

func (db *indexDatabase) Read() (snapshot Snapshot) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	snapshot = &indexSnapshot{
		eav: db.eav.Clone(),
	}
	return
}

func (db *indexDatabase) Write(req Request) (res Response) {
	db.lock.Lock()
	defer db.lock.Unlock()
	panic("TODO")
}

type indexSnapshot struct {
	eav index.Index
}

var _ Snapshot = (*indexSnapshot)(nil)

func (snapshot *indexSnapshot) Select(claim Claim) (datums iterator.Iterator[Datum]) {
	panic("TODO")
}
