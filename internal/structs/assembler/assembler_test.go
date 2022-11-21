package assembler

import (
	"testing"

	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestSimple(t *testing.T) {
	type Person struct {
		Name  string  `attr:"person/name"`
		Title *string `attr:"person/title"`
	}
	facts := []Fact{
		{E: ID(1), A: Ident("person/name"), V: String("Donald")},
		{E: ID(1), A: Ident("person/title"), V: String("Citizen")},
	}
	actual := Person{}
	err := Assemble(&actual, facts)
	assert.NoError(t, err)
	title := "Citizen"
	expected := Person{Name: "Donald", Title: &title}
	assert.Equal(t, expected, actual)
}
