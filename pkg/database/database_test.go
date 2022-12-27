package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabase(t *testing.T) {
	type Person struct {
		Name string `attr:"person/name"`
	}

	db := NewDatabase(Config{})
	res := db.Write(Request{
		Assertions: []any{Person{Name: "Donald"}, Person{Name: "Stephen"}},
	})
	assert.NoError(t, res.Error)
	assert.Len(t, res.IDs, 2)
	for _, id := range res.IDs {
		assert.Positive(t, id)
	}
	// TODO this should work with a nil person pointer
	snapshot := BuildTypedSnapshot(res.Snap, &Person{})
	person, ok := snapshot.Find(res.IDs[0])
	assert.True(t, ok)
	assert.Equal(t, Person{Name: "Donald"}, *person)
}
