// Package database contains the primary database implementation.
package database

import (
	"sync"

	"github.com/dball/destructive/internal/index"
	. "github.com/dball/destructive/internal/types"
)

type indexDatabase struct {
	stringEAVIndex index.Index[string]
	intEAVIndex    index.Index[int64]
	uintEAVIndex   index.Index[uint64]
	floatEAVIndex  index.Index[float64]

	attrsByID    map[ID]Attr
	attrsByIdent map[Ident]Attr

	lock sync.RWMutex
}

var _ Database = (*indexDatabase)(nil)

func NewIndexDatabase(degree int, attrsSize int) Database {
	return &indexDatabase{
		stringEAVIndex: index.NewBTreeIndex(degree, index.LessEAV[string]),
		intEAVIndex:    index.NewBTreeIndex(degree, index.LessEAV[int64]),
		uintEAVIndex:   index.NewBTreeIndex(degree, index.LessEAV[uint64]),
		floatEAVIndex:  index.NewBTreeIndex(degree, index.LessEAV[float64]),
		attrsByID:      make(map[ID]Attr, attrsSize),
		attrsByIdent:   make(map[Ident]Attr, attrsSize),
	}
}

func (db *indexDatabase) Read() (snapshot Snapshot) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return
}

func (db *indexDatabase) Write(req Request) (txn Transaction) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return
}
