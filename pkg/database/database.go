// Package database contains the public database types and functions for destructive.
package database

import (
	"reflect"

	"github.com/dball/destructive/internal/structs/assemblers"
	"github.com/dball/destructive/internal/structs/models"
	"github.com/dball/destructive/internal/types"
)

// Database is a mutable set of data.
type Database interface {
	// Read returns a immutable snapshot of data.
	Read() *Snapshot
	// Write atomically applies the changes in the request to the database.
	Write(req Request) Response
}

// Snapshot is an immutable set of data.
type Snapshot struct {
	snap     types.Snapshot
	analyzer models.Analyzer
}

type typedSnapshot[T any] struct {
	snapshot *Snapshot
	pointer  *T
}

// TypedSnapshot is an immutable set of data that can build instances of specific
// struct types.
type TypedSnapshot[T any] interface {
	Find(id uint64) (entity *T, found bool)
}

func (ts *typedSnapshot[T]) Find(id uint64) (entity *T, found bool) {
	val := reflect.ValueOf(ts.pointer)
	if val.Kind() != reflect.Pointer {
		return
	}
	entityStruct := val.Elem()
	_, err := models.Analyze(entityStruct.Type())
	if err != nil {
		return
	}
	assembler := assemblers.NewAssembler(ts.snapshot.analyzer, ts.snapshot.snap)
	assembled, err := assemblers.Assemble(assembler, types.ID(id), ts.pointer)
	if err != nil {
		return
	}
	entity = &assembled
	found = true
	return
}

func BuildTypedSnapshot[T any](snapshot *Snapshot, pointer *T) (ts TypedSnapshot[T]) {
	// TODO validate pointer is to a struct with attr tags
	return &typedSnapshot[T]{snapshot: snapshot, pointer: pointer}
}

// Request specifies changes to apply to a database. If a Request is written
// successfully, any id fields of the entities that comprise it will be populated.
type Request struct {
	// Assertions is a list of entities whose attr tag fields will be present in the
	// database after a successful write.
	Assertions []any
	// Retractions is a list of entities whose attributes will be retracted from the
	// database after a successful write.
	Retractions []any
	// Transaction is an entity which, if given, provides attr tag fields that will be
	// asserted on the transaction of a successful write.
	Transaction any
}

// Response specifies the results of trying to write a request to a database.
type Response struct {
	// Transaction is the entity representation of the transaction, if successful. This will
	// be the referent entity of the request if one was given.
	Transaction any
	// Snap is the immutable set of data after the request was written, or when it was rejected.
	Snap *Snapshot
	// Error specifies why a request was rejected.
	Error error
	// IDs contains the list of ids of the asserted entities in the same order.
	IDs []uint64
}
