package assembler

import (
	"testing"
	"time"

	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestSimple2(t *testing.T) {
	type Person struct {
		Name         string     `attr:"person/name"`
		Title        *string    `attr:"person/title"`
		CatCount     int        `attr:"person/cat-count"`
		DogCount     *int       `attr:"person/dog-count"`
		LikesPizza   bool       `attr:"person/likes-pizza"`
		LikesPickles *bool      `attr:"person/likes-pickles"`
		Score        float64    `attr:"person/score"`
		TopScore     *float64   `attr:"person/top-score"`
		Born         time.Time  `attr:"person/born"`
		Died         *time.Time `attr:"person/died"`
	}
	var p *Person

	t.Run("no facts", func(t *testing.T) {
		assembler, err := NewAssembler(p, []Fact{})
		assert.NoError(t, err)
		actual, err := assembler.Next()
		assert.NoError(t, err)
		assert.Nil(t, actual)
	})

	t.Run("one entity of facts", func(t *testing.T) {
		born := time.Date(1969, 7, 20, 20, 17, 54, 0, time.UTC)
		died := time.Date(1986, 1, 28, 16, 39, 13, 0, time.UTC)
		facts := []Fact{
			{E: ID(1), A: Ident("person/name"), V: String("Donald")},
			{E: ID(1), A: Ident("person/title"), V: String("Citizen")},
			{E: ID(1), A: Ident("person/cat-count"), V: Int(4)},
			{E: ID(1), A: Ident("person/dog-count"), V: Int(0)},
			{E: ID(1), A: Ident("person/likes-pizza"), V: Bool(true)},
			{E: ID(1), A: Ident("person/likes-pickles"), V: Bool(true)},
			{E: ID(1), A: Ident("person/score"), V: Float(2.3)},
			{E: ID(1), A: Ident("person/top-score"), V: Float(4.2)},
			{E: ID(1), A: Ident("person/born"), V: Inst(born)},
			{E: ID(1), A: Ident("person/died"), V: Inst(died)},
		}
		assembler, err := NewAssembler(p, facts)
		assert.NoError(t, err)
		actual, err := assembler.Next()
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		// TODO populate
		assert.Equal(t, Person{}, *actual)
	})
}

func TestSimple(t *testing.T) {
	type Person struct {
		Name         string     `attr:"person/name"`
		Title        *string    `attr:"person/title"`
		CatCount     int        `attr:"person/cat-count"`
		DogCount     *int       `attr:"person/dog-count"`
		LikesPizza   bool       `attr:"person/likes-pizza"`
		LikesPickles *bool      `attr:"person/likes-pickles"`
		Score        float64    `attr:"person/score"`
		TopScore     *float64   `attr:"person/top-score"`
		Born         time.Time  `attr:"person/born"`
		Died         *time.Time `attr:"person/died"`
	}
	born := time.Date(1969, 7, 20, 20, 17, 54, 0, time.UTC)
	died := time.Date(1986, 1, 28, 16, 39, 13, 0, time.UTC)
	facts := []Fact{
		{E: ID(1), A: Ident("person/name"), V: String("Donald")},
		{E: ID(1), A: Ident("person/title"), V: String("Citizen")},
		{E: ID(1), A: Ident("person/cat-count"), V: Int(4)},
		{E: ID(1), A: Ident("person/dog-count"), V: Int(0)},
		{E: ID(1), A: Ident("person/likes-pizza"), V: Bool(true)},
		{E: ID(1), A: Ident("person/likes-pickles"), V: Bool(true)},
		{E: ID(1), A: Ident("person/score"), V: Float(2.3)},
		{E: ID(1), A: Ident("person/top-score"), V: Float(4.2)},
		{E: ID(1), A: Ident("person/born"), V: Inst(born)},
		{E: ID(1), A: Ident("person/died"), V: Inst(died)},
	}
	actual := Person{}
	unused, err := Assemble(&actual, facts)
	assert.NoError(t, err)
	assert.Empty(t, unused)
	title := "Citizen"
	zero := 0
	yes := true
	fourPointTwo := 4.2
	expected := Person{
		Name:         "Donald",
		Title:        &title,
		CatCount:     4,
		DogCount:     &zero,
		LikesPizza:   true,
		LikesPickles: &yes,
		Score:        2.3,
		TopScore:     &fourPointTwo,
		Born:         born,
		Died:         &died,
	}
	assert.Equal(t, expected, actual)
}

/*
func TestStructs(t *testing.T) {
	type Book struct {
		Title string `attr:"book/title"`
	}

	type Person struct {
		Name     string `attr:"person/name"`
		Favorite Book   `attr:"person/favorite-book"`
	}

	t.Run("value struct field", func(t *testing.T) {
		facts := []Fact{
			{E: ID(1), A: Ident("person/name"), V: String("Donald")},
			{E: ID(1), A: Ident("person/favorite-book"), V: ID(2)},
			{E: ID(2), A: Ident("book/title"), V: String("Immortality")},
		}
		actual := Person{}
		unused, err := Assemble(&actual, facts)
		assert.NoError(t, err)
		assert.Empty(t, unused)
		expected := Person{Name: "Donald", Favorite: Book{Title: "Immortality"}}
		assert.Equal(t, expected, actual)
	})
}
*/

/*
func TestMapWithValues(t *testing.T) {
	type Book struct {
		Title string `attr:"book/title"`
		Genre string `attr:"book/genre"`
	}
	type Person struct {
		Name string          `attr:"person/name"`
		Favs map[string]Book `attr:"person/favs,mapKey=book/title"`
	}
	facts := []Fact{
		{E: ID(1), A: Ident("person/name"), V: String("Donald")},
		{E: ID(1), A: Ident("person/favs"), V: ID(2)},
		{E: ID(1), A: Ident("person/favs"), V: ID(3)},
		{E: ID(2), A: Ident("book/title"), V: String("Legendborn")},
		{E: ID(2), A: Ident("book/genre"), V: String("ya")},
		{E: ID(3), A: Ident("book/title"), V: String("The Actual Star")},
		{E: ID(3), A: Ident("book/genre"), V: String("specfic")},
	}
	actual := Person{}
	err := Assemble(&actual, facts)
	assert.NoError(t, err)
	expected := Person{
		Name: "Donald",
		Favs: map[string]Book{
			"Legendborn":      {Title: "Legendborn", Genre: "ya"},
			"The Actual Star": {Title: "The Actual Star", Genre: "specfic"},
		},
	}
	assert.Equal(t, expected, actual)
}
*/
