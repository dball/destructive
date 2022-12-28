package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabase(t *testing.T) {
	type Person struct {
		// TODO uint64 obvsly
		ID   uint   `attr:"sys/db/id"`
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

	t.Run("find person", func(t *testing.T) {
		// TODO this should work with a nil person pointer
		snapshot := BuildTypedSnapshot(res.Snap, &Person{})
		person, ok := snapshot.Find(res.IDs[0])
		assert.True(t, ok)
		assert.Equal(t, Person{ID: uint(res.IDs[0]), Name: "Donald"}, *person)
	})

	t.Run("find different struct with overlapping field", func(t *testing.T) {
		type Named struct {
			PersonName string `attr:"person/name"`
		}

		snapshot := BuildTypedSnapshot(res.Snap, &Named{})
		named, ok := snapshot.Find(res.IDs[1])
		assert.True(t, ok)
		assert.Equal(t, Named{PersonName: "Stephen"}, *named)
	})

	t.Run("rename person changes future reads but not past reads", func(t *testing.T) {
		id := res.IDs[0]
		snapshot1 := BuildTypedSnapshot(res.Snap, &Person{})
		res = db.Write(Request{
			Assertions: []any{Person{ID: uint(id), Name: "Donato"}},
		})
		assert.NoError(t, res.Error)
		snapshot2 := BuildTypedSnapshot(res.Snap, &Person{})
		person, ok := snapshot2.Find(id)
		assert.True(t, ok)
		assert.Equal(t, Person{ID: uint(id), Name: "Donato"}, *person)
		person, ok = snapshot1.Find(id)
		assert.True(t, ok)
		assert.Equal(t, Person{ID: uint(id), Name: "Donald"}, *person)
	})
}
