package shredder

import (
	"testing"
	"time"

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
		// struct fields must be public to be shredded unless we go unsafe
		Birthdate time.Time  `attr:"person/birthdate,ignoreempty"`
		Deathdate *time.Time `attr:"person/deathdate"`
	}

	t.Run("assert", func(t *testing.T) {
		shredder := NewShredder()
		epoch := time.Date(1969, 7, 20, 20, 17, 54, 0, time.UTC)
		txn, err := shredder.Assert(person{id: 23, name: "Donald", age: 48, Birthdate: epoch})
		assert.NoError(t, err)
		expected := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
				{E: TempID("1"), A: Ident("person/age"), V: Int(48)},
				{E: TempID("1"), A: Ident("person/birthdate"), V: Inst(epoch)},
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
		epoch := time.Date(1969, 7, 20, 20, 17, 54, 0, time.UTC)
		txn, err := shredder.Assert(person{name: "Donald", pets: &four, Deathdate: &epoch})
		assert.NoError(t, err)
		expected := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
				{E: TempID("1"), A: Ident("person/pets"), V: Int(4)},
				{E: TempID("1"), A: Ident("person/deathdate"), V: Inst(epoch)},
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

func TestRefs(t *testing.T) {
	type Person struct {
		Name string  `attr:"person/name"`
		BFF  *Person `attr:"person/bff"`
	}

	t.SkipNow()

	t.Run("two mutuals", func(t *testing.T) {
		shredder := NewShredder()
		momo := Person{Name: "Momo"}
		pabu := Person{Name: "Pabu"}
		momo.BFF = &pabu
		pabu.BFF = &momo
		actual, err := shredder.Assert(momo)
		assert.NoError(t, err)
		expected := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("person/name"), V: String("Momo")},
				{E: TempID("1"), A: Ident("person/bff"), V: TempID("2")},
				{E: TempID("2"), A: Ident("person/name"), V: String("Pabu")},
				{E: TempID("2"), A: Ident("person/bff"), V: TempID("1")},
			},
			TempIDs: map[TempID]map[IDRef]Void{
				TempID("1"): {},
				TempID("2"): {},
			},
		}
		assert.Equal(t, expected, actual)
	})

}
