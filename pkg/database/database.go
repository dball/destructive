// Package database contains the public database types and functions for destructive.
package database

// Database is a mutable set of data.
type Database interface {
	// Read returns a immutable snapshot of data.
	Read() Snapshot
	// Write atomically applies the changes in the request to the database.
	Write(req Request) Response
}

// Snapshot is an immutable set of data.
type Snapshot interface {
	// Find populates the struct referent of the given entity, if the attr fields
	// thereof resolve to a single identity.
	Find(entity any) (found bool)
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
	// Snapshot is the immutable set of data after the request was written, or when it was rejected.
	Snapshot Snapshot
	// Error specifies why a request was rejected.
	Error error
}
