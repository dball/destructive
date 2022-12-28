package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabase(t *testing.T) {
	type Person struct {
		ID   uint64 `attr:"sys/db/id"`
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
		snapshot, err := BuildTypedSnapshot(res.Snap, (*Person)(nil))
		assert.NoError(t, err)
		person, ok := snapshot.Find(res.IDs[0])
		assert.True(t, ok)
		assert.Equal(t, Person{ID: res.IDs[0], Name: "Donald"}, *person)
	})

	t.Run("find different struct with overlapping field", func(t *testing.T) {
		type Named struct {
			PersonName string `attr:"person/name"`
		}

		snapshot, err := BuildTypedSnapshot(res.Snap, &Named{})
		assert.NoError(t, err)
		named, ok := snapshot.Find(res.IDs[1])
		assert.True(t, ok)
		assert.Equal(t, Named{PersonName: "Stephen"}, *named)
	})

	t.Run("rename person changes future reads but not past reads", func(t *testing.T) {
		id := res.IDs[0]
		snapshot1, err := BuildTypedSnapshot(res.Snap, &Person{})
		assert.NoError(t, err)
		res = db.Write(Request{
			Assertions: []any{Person{ID: id, Name: "Donato"}},
		})
		assert.NoError(t, res.Error)
		snapshot2, err := BuildTypedSnapshot(res.Snap, &Person{})
		assert.NoError(t, err)
		person, ok := snapshot2.Find(id)
		assert.True(t, ok)
		assert.Equal(t, Person{ID: id, Name: "Donato"}, *person)
		person, ok = snapshot1.Find(id)
		assert.True(t, ok)
		assert.Equal(t, Person{ID: id, Name: "Donald"}, *person)
	})

	t.Run("assert accepts struct pointers as well as structs literals", func(t *testing.T) {
		res = db.Write(Request{
			Assertions: []any{&Person{Name: "Octavia"}},
		})
		assert.NoError(t, res.Error)
		assert.Len(t, res.IDs, 1)
	})

	t.Run("retract accepts a struct", func(t *testing.T) {
		id := res.IDs[0]
		res := db.Write(Request{
			Retractions: []any{Person{ID: id}},
		})
		assert.NoError(t, res.Error)
		snapshot, err := BuildTypedSnapshot(res.Snap, (*Person)(nil))
		assert.NoError(t, err)
		person, ok := snapshot.Find(id)
		assert.False(t, ok)
		assert.Nil(t, person)
	})
}
