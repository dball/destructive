package shredder

import (
	"testing"

	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestShred(t *testing.T) {
	type person struct {
		id   uint   `attr:"sys/db/id"`
		name string `attr:"person/name,unique"`
		uuid string `attr:"person/uuid,identity,ignoreempty"`
		age  int    `attr:"person/age,ignoreempty"`
		pets *int   `attr:"person/pets"`
	}

	t.Run("retract", func(t *testing.T) {
		shredder := NewShredder()
		txn, err := shredder.Retract(person{id: 23, name: "Donald", age: 48})
		assert.NoError(t, err)
		expected := Request{
			Claims: []*Claim{{E: TempID("1"), A: nil, V: nil, Retract: true}},
			TempIDs: map[TempID]map[IDRef]Void{
				TempID("1"): {
					ID(23): Void{},
					LookupRef{A: Ident("person/name"), V: String("Donald")}: Void{},
				},
			},
		}
		assert.Equal(t, expected, txn)
	})

	t.Run("assert", func(t *testing.T) {
		shredder := NewShredder()
		txn, err := shredder.Assert(person{id: 23, name: "Donald", age: 48})
		assert.NoError(t, err)
		expected := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
				{E: TempID("1"), A: Ident("person/age"), V: Int(48)},
			},
			TempIDs: map[TempID]map[IDRef]Void{
				TempID("1"): {
					ID(23): Void{},
					LookupRef{A: Ident("person/name"), V: String("Donald")}: Void{},
				},
			},
		}
		assert.Equal(t, expected, txn)
	})

	t.Run("non-empty uuid", func(t *testing.T) {
		shredder := NewShredder()
		txn, err := shredder.Assert(person{id: 23, name: "Donald", uuid: "the-uuid"})
		assert.NoError(t, err)
		expected := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
				{E: TempID("1"), A: Ident("person/uuid"), V: String("the-uuid")},
			},
			TempIDs: map[TempID]map[IDRef]Void{
				TempID("1"): {
					ID(23): Void{},
					LookupRef{A: Ident("person/name"), V: String("Donald")}:   Void{},
					LookupRef{A: Ident("person/uuid"), V: String("the-uuid")}: Void{},
				},
			},
		}
		assert.Equal(t, expected, txn)
	})

	t.Run("pointer value", func(t *testing.T) {
		shredder := NewShredder()
		four := 4
		txn, err := shredder.Assert(person{name: "Donald", pets: &four})
		assert.NoError(t, err)
		expected := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
				{E: TempID("1"), A: Ident("person/pets"), V: Int(4)},
			},
			TempIDs: map[TempID]map[IDRef]Void{
				TempID("1"): {
					LookupRef{A: Ident("person/name"), V: String("Donald")}: Void{},
				},
			},
		}
		assert.Equal(t, expected, txn)

	})

	t.Run("invalid values", func(t *testing.T) {
		shredder := NewShredder()
		_, err := shredder.Assert(5)
		assert.Error(t, err)
	})
}
