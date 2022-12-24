// Package database contains the public database types and functions for destructive.
package database

// Entity is a struct with any number of attr fields.
type Entity = any

// EntityPointer is a pointer to an entity. This is typically used to convey the
// attr tag metadata rather than as a mutable reference.
type EntityPointer = *Entity

// Database is a mutable set of data.
type Database interface {
	// Read returns a immutable snapshot of data.
	Read() Snapshot
	// Write atomically applies the changes in the request to the database.
	Write(req Request) Response
}

// Snapshot is an immutable set of data.
type Snapshot interface {
	Populate(entityPointer EntityPointer) (found bool)
	Find(entityPointer EntityPointer, id uint64) (entity Entity, found bool)
	FindUnique(entityPointer EntityPointer, attr string, value any) (entity Entity, found bool)
}

type typedSnapshot[T any] struct {
	snapshot Snapshot
	pointer  *T
}

// TypedSnapshot is an immutable set of data that can build instances of specific
// struct types.
type TypedSnapshot[T any] interface {
	Find(id uint64) (entity *T, found bool)
}

func (ts *typedSnapshot[T]) Find(id uint64) (entity *T, found bool) {
	panic("TODO")
}

func BuildTypedSnapshot[T any](snapshot Snapshot, pointer *T) (ts TypedSnapshot[T]) {
	// TODO validate pointer is to a struct with attr tags
	return &typedSnapshot[T]{snapshot: snapshot, pointer: pointer}
}

// Request specifies changes to apply to a database. If a Request is written
// successfully, any id fields of the entities that comprise it will be populated.
type Request struct {
	// Assertions is a list of entities whose attr tag fields will be present in the
	// database after a successful write.
	Assertions []Entity
	// Retractions is a list of entities whose attributes will be retracted from the
	// database after a successful write.
	Retractions []Entity
	// Transaction is an entity which, if given, provides attr tag fields that will be
	// asserted on the transaction of a successful write.
	Transaction Entity
}

// Response specifies the results of trying to write a request to a database.
type Response struct {
	// Transaction is the entity representation of the transaction, if successful. This will
	// be the referent entity of the request if one was given.
	Transaction Entity
	// Snapshot is the immutable set of data after the request was written, or when it was rejected.
	Snapshot Snapshot
	// Error specifies why a request was rejected.
	Error error
}
