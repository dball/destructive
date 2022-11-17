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
		p := person{id: 23, name: "Donald", age: 48, Birthdate: epoch}
		req, err := shredder.Shred(Document{Assertions: []any{p}})
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
		assert.Equal(t, expected, req)
	})

	t.Run("retract", func(t *testing.T) {
		shredder := NewShredder()
		p := person{id: 23, name: "Donald", age: 48}
		req, err := shredder.Shred(Document{Retractions: []any{p}})
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
		assert.Equal(t, expected, req)
	})

	t.Run("non-empty uuid", func(t *testing.T) {
		shredder := NewShredder()
		p := person{id: 23, name: "Donald", uuid: "the-uuid"}
		req, err := shredder.Shred(Document{Assertions: []any{p}})
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
		assert.Equal(t, expected, req)
	})

	t.Run("pointer value", func(t *testing.T) {
		shredder := NewShredder()
		four := 4
		epoch := time.Date(1969, 7, 20, 20, 17, 54, 0, time.UTC)
		p := person{name: "Donald", pets: &four, Deathdate: &epoch}
		req, err := shredder.Shred(Document{Assertions: []any{p}})
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
		assert.Equal(t, expected, req)

	})

	t.Run("invalid values", func(t *testing.T) {
		shredder := NewShredder()
		_, err := shredder.Shred(Document{Assertions: []any{5}})
		assert.Error(t, err)
	})
}

func TestRefs(t *testing.T) {
	type Person struct {
		Name string  `attr:"person/name"`
		BFF  *Person `attr:"person/bff"`
	}

	t.Run("two mutuals", func(t *testing.T) {
		shredder := NewShredder()
		momo := Person{Name: "Momo"}
		pabu := Person{Name: "Pabu"}
		momo.BFF = &pabu
		pabu.BFF = &momo
		actual, err := shredder.Shred(Document{Assertions: []any{&momo, &pabu}})
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
