// Package database contains the primary database implementation.
package database

import (
	"sync"

	"github.com/dball/destructive/internal/index"
	. "github.com/dball/destructive/internal/types"
)

type indexDatabase struct {
	eav *index.CompositeIndex

	attrsByID    map[ID]Attr
	attrsByIdent map[Ident]Attr
	attrTypes    map[ID]ID

	lock sync.RWMutex
}

var _ Database = (*indexDatabase)(nil)

func NewIndexDatabase(degree int, attrsSize int) (db Database) {
	attrTypes := make(map[ID]ID, attrsSize)
	db = &indexDatabase{
		eav:          index.NewCompositeIndex(degree, index.EAVIndex, attrTypes),
		attrsByID:    make(map[ID]Attr, attrsSize),
		attrsByIdent: make(map[Ident]Attr, attrsSize),
		attrTypes:    attrTypes,
	}
	return
}

func (db *indexDatabase) Read() (snapshot Snapshot) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return
}

func (db *indexDatabase) Write(req Request) (res Response) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return
}
