package assembler

import (
	"testing"

	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestSimple(t *testing.T) {
	type Person struct {
		Name string `attr:"person/name"`
	}
	facts := []Fact{
		{E: ID(1), A: Ident("person/name"), V: String("Donald")},
	}
	actual := Person{}
	err := Assemble(&actual, facts)
	assert.NoError(t, err)
	expected := Person{Name: "Donald"}
	assert.Equal(t, expected, actual)
}
