package assembler

import (
	"testing"
	"time"

	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestSimple(t *testing.T) {
	type Person struct {
		ID           uint64     `attr:"sys/db/id"`
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
		title := "Citizen"
		zero := 0
		yes := true
		fourPointTwo := 4.2
		expected := Person{
			ID:           1,
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
		assert.Equal(t, expected, *actual)

		// yields only one entity
		actual, err = assembler.Next()
		assert.NoError(t, err)
		assert.Nil(t, actual)
	})
}

func TestStructs(t *testing.T) {
	type Book struct {
		Title string `attr:"book/title"`
	}

	type Person struct {
		Name     string `attr:"person/name"`
		Favorite Book   `attr:"person/favorite-book"`
		Best     *Book  `attr:"person/best-book"`
	}

	var p *Person

	t.Run("struct fields", func(t *testing.T) {
		facts := []Fact{
			{E: ID(1), A: Ident("person/name"), V: String("Donald")},
			{E: ID(1), A: Ident("person/favorite-book"), V: ID(2)},
			{E: ID(1), A: Ident("person/best-book"), V: ID(3)},
			{E: ID(2), A: Ident("book/title"), V: String("Immortality")},
			{E: ID(3), A: Ident("book/title"), V: String("The Parable of the Sower")},
		}
		assembler, err := NewAssembler(p, facts)
		assert.NoError(t, err)
		actual, err := assembler.Next()
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		expected := Person{Name: "Donald", Favorite: Book{Title: "Immortality"}, Best: &Book{Title: "The Parable of the Sower"}}
		assert.Equal(t, expected, *actual)
	})
}

func TestStructCycles(t *testing.T) {
	type Person struct {
		Name string  `attr:"person/name"`
		BFF  *Person `attr:"person/bff"`
	}
	var p *Person
	facts := []Fact{
		{E: ID(1), A: Ident("person/name"), V: String("Momo")},
		{E: ID(1), A: Ident("person/bff"), V: ID(2)},
		{E: ID(2), A: Ident("person/name"), V: String("Pabu")},
		{E: ID(2), A: Ident("person/bff"), V: ID(1)},
	}
	assembler, err := NewAssembler(p, facts)
	assert.NoError(t, err)
	actual, err := assembler.Next()
	assert.NoError(t, err)
	assert.NotNil(t, actual)
	// We cannot assert equality on the structs themselves because they
	// mutually refer.
	assert.Equal(t, "Momo", actual.Name)
	assert.Equal(t, "Pabu", actual.BFF.Name)
	assert.Equal(t, "Momo", actual.BFF.BFF.Name)

	actual, err = assembler.Next()
	assert.NoError(t, err)
	assert.Equal(t, "Pabu", actual.Name)
	assert.Equal(t, "Momo", actual.BFF.Name)
	assert.Equal(t, "Pabu", actual.BFF.BFF.Name)
}

func TestMapWithStructValues(t *testing.T) {
	type Book struct {
		Title string `attr:"book/title"`
		Genre string `attr:"book/genre"`
	}
	type Person struct {
		Name string          `attr:"person/name"`
		Favs map[string]Book `attr:"person/favs,key=book/title"`
	}

	var p *Person
	facts := []Fact{
		{E: ID(1), A: Ident("person/name"), V: String("Donald")},
		{E: ID(1), A: Ident("person/favs"), V: ID(2)},
		{E: ID(1), A: Ident("person/favs"), V: ID(3)},
		{E: ID(2), A: Ident("book/genre"), V: String("ya")},
		{E: ID(2), A: Ident("book/title"), V: String("Legendborn")},
		{E: ID(3), A: Ident("book/genre"), V: String("specfic")},
		{E: ID(3), A: Ident("book/title"), V: String("The Actual Star")},
	}
	assembler, err := NewAssembler(p, facts)
	assert.NoError(t, err)
	actual, err := assembler.Next()
	assert.NoError(t, err)
	assert.NotNil(t, actual)
	assert.Equal(t, "Donald", actual.Name)
	assert.Equal(t, 2, len(actual.Favs))
	assert.Equal(t, Book{Title: "Legendborn", Genre: "ya"}, actual.Favs["Legendborn"])
	assert.Equal(t, Book{Title: "The Actual Star", Genre: "specfic"}, actual.Favs["The Actual Star"])
}

func TestMapWithPointerValues(t *testing.T) {
	type Book struct {
		Title string `attr:"book/title"`
		Genre string `attr:"book/genre"`
	}
	type Person struct {
		Name string           `attr:"person/name"`
		Favs map[string]*Book `attr:"person/favs,key=book/title"`
	}

	var p *Person
	facts := []Fact{
		{E: ID(1), A: Ident("person/name"), V: String("Donald")},
		{E: ID(1), A: Ident("person/favs"), V: ID(2)},
		{E: ID(1), A: Ident("person/favs"), V: ID(3)},
		{E: ID(2), A: Ident("book/genre"), V: String("ya")},
		{E: ID(2), A: Ident("book/title"), V: String("Legendborn")},
		{E: ID(3), A: Ident("book/genre"), V: String("specfic")},
		{E: ID(3), A: Ident("book/title"), V: String("The Actual Star")},
	}
	assembler, err := NewAssembler(p, facts)
	assert.NoError(t, err)
	actual, err := assembler.Next()
	assert.NoError(t, err)
	assert.NotNil(t, actual)
	assert.Equal(t, "Donald", actual.Name)
	assert.Equal(t, 2, len(actual.Favs))
	book := actual.Favs["Legendborn"]
	assert.NotNil(t, book)
	assert.Equal(t, Book{Title: "Legendborn", Genre: "ya"}, *book)
	book = actual.Favs["The Actual Star"]
	assert.Equal(t, Book{Title: "The Actual Star", Genre: "specfic"}, *book)
}

func TestSliceOfStructValues(t *testing.T) {
	type Book struct {
		Title string `attr:"book/title"`
	}

	type Person struct {
		Name string `attr:"person/name"`
		Favs []Book `attr:"person/favs"`
	}

	// TODO note this schema requires the referents can only be ranked by one referrer.
	// One could imagine a ranking entity with refs to the referrer, the referent, and
	// the rank as a value, but that would require bidirectional refs.
	var p *Person
	facts := []Fact{
		{E: ID(1), A: Ident("person/name"), V: String("Donald")},
		{E: ID(1), A: Ident("person/favs"), V: ID(2)},
		{E: ID(1), A: Ident("person/favs"), V: ID(3)},
		{E: ID(2), A: Ident("book/title"), V: String("Legendborn")},
		{E: ID(2), A: Ident("sys/db/rank"), V: Int(1)},
		{E: ID(3), A: Ident("book/title"), V: String("The Actual Star")},
		{E: ID(3), A: Ident("sys/db/rank"), V: Int(0)},
	}
	assembler, err := NewAssembler(p, facts)
	assert.NoError(t, err)
	actual, err := assembler.Next()
	assert.NoError(t, err)
	assert.NotNil(t, actual)
	expected := Person{
		Name: "Donald",
		Favs: []Book{
			{Title: "The Actual Star"},
			{Title: "Legendborn"},
		},
	}
	assert.Equal(t, expected, *actual)
}
