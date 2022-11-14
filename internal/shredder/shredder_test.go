package shredder

import (
	"testing"

	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestShred(t *testing.T) {
	shredder := NewShredder()
	type person struct {
		id   uint   `attr:"sys/db/id"`
		name string `attr:"person/name,unique"`
		uuid string `attr:"person/uuid,identity"`
		age  int    `attr:"person/age"`
	}
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
}
