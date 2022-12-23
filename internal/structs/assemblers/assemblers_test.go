package assemblers

import (
	"reflect"
	"testing"
	"time"

	"github.com/dball/destructive/internal/database"
	"github.com/dball/destructive/internal/structs/models"
	"github.com/dball/destructive/internal/structs/schemas"
	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func buildComponents(t *testing.T, structInstances ...any) (analyzer models.Analyzer, db Database) {
	db = database.NewIndexDatabase(32, 64, 64)
	analyzer = models.BuildCachingAnalyzer()
	for _, structInstance := range structInstances {
		claims, err := schemas.Analyze(reflect.TypeOf(structInstance))
		assert.NoError(t, err)
		res := db.Write(Request{Claims: claims})
		assert.NoError(t, res.Error)
	}
	return
}

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

	analyzer, db := buildComponents(t, Person{})

	born := time.Date(1969, 7, 20, 20, 17, 54, 0, time.UTC)
	died := time.Date(1986, 1, 28, 16, 39, 13, 0, time.UTC)
	req := Request{
		Claims: []Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/title"), V: String("Citizen")},
			{E: TempID("1"), A: Ident("person/cat-count"), V: Int(4)},
			{E: TempID("1"), A: Ident("person/dog-count"), V: Int(0)},
			{E: TempID("1"), A: Ident("person/likes-pizza"), V: Bool(true)},
			{E: TempID("1"), A: Ident("person/likes-pickles"), V: Bool(true)},
			{E: TempID("1"), A: Ident("person/score"), V: Float(2.3)},
			{E: TempID("1"), A: Ident("person/top-score"), V: Float(4.2)},
			{E: TempID("1"), A: Ident("person/born"), V: Inst(born)},
			{E: TempID("1"), A: Ident("person/died"), V: Inst(died)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("1")]
	assert.Positive(t, id)
	assembler := NewAssembler(analyzer, res.Snapshot)
	entity, err := Assemble(assembler, id, (*Person)(nil))
	assert.NoError(t, err)
	title := "Citizen"
	zero := 0
	yes := true
	fourPointTwo := 4.2
	expected := Person{
		ID:           uint64(id),
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
	assert.Equal(t, expected, entity)
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

	analyzer, db := buildComponents(t, Person{})

	req := Request{
		Claims: []Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/favorite-book"), V: TempID("2")},
			{E: TempID("1"), A: Ident("person/best-book"), V: TempID("3")},
			{E: TempID("2"), A: Ident("book/title"), V: String("Immortality")},
			{E: TempID("3"), A: Ident("book/title"), V: String("The Parable of the Sower")},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	id := res.TempIDs[TempID("1")]

	assembler := NewAssembler(analyzer, res.Snapshot)
	entity, err := Assemble(assembler, id, (*Person)(nil))
	assert.NoError(t, err)
	expected := Person{Name: "Donald", Favorite: Book{Title: "Immortality"}, Best: &Book{Title: "The Parable of the Sower"}}
	assert.Equal(t, expected, entity)
}

func TestStructCycles(t *testing.T) {
	type Person struct {
		Name string  `attr:"person/name"`
		BFF  *Person `attr:"person/bff"`
	}

	analyzer, db := buildComponents(t, Person{})
	req := Request{
		Claims: []Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Momo")},
			{E: TempID("1"), A: Ident("person/bff"), V: TempID("2")},
			{E: TempID("2"), A: Ident("person/name"), V: String("Pabu")},
			{E: TempID("2"), A: Ident("person/bff"), V: TempID("1")},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	momo := res.TempIDs[TempID("1")]
	pabu := res.TempIDs[TempID("2")]

	assembler := NewAssembler(analyzer, res.Snapshot)
	entity, err := Assemble(assembler, momo, (*Person)(nil))
	assert.NoError(t, err)

	// We cannot assert equality on the structs themselves because they
	// mutually refer.
	assert.Equal(t, "Momo", entity.Name)
	assert.Equal(t, "Pabu", entity.BFF.Name)
	assert.Equal(t, "Momo", entity.BFF.BFF.Name)

	entity, err = Assemble(assembler, pabu, (*Person)(nil))
	assert.NoError(t, err)
	assert.Equal(t, "Pabu", entity.Name)
	assert.Equal(t, "Momo", entity.BFF.Name)
	assert.Equal(t, "Pabu", entity.BFF.BFF.Name)
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

	analyzer, db := buildComponents(t, Person{})
	req := Request{
		Claims: []Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/favs"), V: TempID("2")},
			{E: TempID("1"), A: Ident("person/favs"), V: TempID("3")},
			{E: TempID("2"), A: Ident("book/genre"), V: String("ya")},
			{E: TempID("2"), A: Ident("book/title"), V: String("Legendborn")},
			{E: TempID("3"), A: Ident("book/genre"), V: String("specfic")},
			{E: TempID("3"), A: Ident("book/title"), V: String("The Actual Star")},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	assembler := NewAssembler(analyzer, res.Snapshot)
	entity, err := Assemble(assembler, res.TempIDs[TempID("1")], (*Person)(nil))
	assert.NoError(t, err)

	assert.Equal(t, "Donald", entity.Name)
	assert.Equal(t, 2, len(entity.Favs))
	assert.Equal(t, Book{Title: "Legendborn", Genre: "ya"}, entity.Favs["Legendborn"])
	assert.Equal(t, Book{Title: "The Actual Star", Genre: "specfic"}, entity.Favs["The Actual Star"])
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

	analyzer, db := buildComponents(t, Person{})
	req := Request{
		Claims: []Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/favs"), V: TempID("2")},
			{E: TempID("1"), A: Ident("person/favs"), V: TempID("3")},
			{E: TempID("2"), A: Ident("book/genre"), V: String("ya")},
			{E: TempID("2"), A: Ident("book/title"), V: String("Legendborn")},
			{E: TempID("3"), A: Ident("book/genre"), V: String("specfic")},
			{E: TempID("3"), A: Ident("book/title"), V: String("The Actual Star")},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	assembler := NewAssembler(analyzer, res.Snapshot)
	entity, err := Assemble(assembler, res.TempIDs[TempID("1")], (*Person)(nil))
	assert.NoError(t, err)

	assert.Equal(t, "Donald", entity.Name)
	assert.Equal(t, 2, len(entity.Favs))
	assert.Equal(t, Book{Title: "Legendborn", Genre: "ya"}, *entity.Favs["Legendborn"])
	assert.Equal(t, Book{Title: "The Actual Star", Genre: "specfic"}, *entity.Favs["The Actual Star"])
}

func TestSliceOfStructValues(t *testing.T) {
	type Book struct {
		Title string `attr:"book/title"`
	}

	type Person struct {
		Name string `attr:"person/name"`
		Favs []Book `attr:"person/favs"`
	}

	analyzer, db := buildComponents(t, Person{})
	req := Request{
		Claims: []Claim{
			{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
			{E: TempID("1"), A: Ident("person/favs"), V: TempID("2")},
			{E: TempID("1"), A: Ident("person/favs"), V: TempID("3")},
			{E: TempID("2"), A: Ident("book/title"), V: String("Legendborn")},
			{E: TempID("2"), A: Ident("sys/db/rank"), V: Int(1)},
			{E: TempID("3"), A: Ident("book/title"), V: String("The Actual Star")},
			{E: TempID("3"), A: Ident("sys/db/rank"), V: Int(0)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	assembler := NewAssembler(analyzer, res.Snapshot)
	entity, err := Assemble(assembler, res.TempIDs[TempID("1")], (*Person)(nil))
	assert.NoError(t, err)

	expected := Person{
		Name: "Donald",
		Favs: []Book{
			{Title: "The Actual Star"},
			{Title: "Legendborn"},
		},
	}
	assert.Equal(t, expected, entity)
}

func TestSliceOfScalarValues(t *testing.T) {
	type Test struct {
		Title  string    `attr:"test/title"`
		Scores []float64 `attr:"test/scores,value=test/score"`
	}

	analyzer, db := buildComponents(t, Test{})
	req := Request{
		Claims: []Claim{
			{E: TempID("1"), A: Ident("test/scores"), V: TempID("2")},
			{E: TempID("1"), A: Ident("test/scores"), V: TempID("3")},
			{E: TempID("1"), A: Ident("test/scores"), V: TempID("4")},
			{E: TempID("1"), A: Ident("test/title"), V: String("Algebra II")},
			{E: TempID("2"), A: Ident("test/score"), V: Float(95.3)},
			{E: TempID("2"), A: Ident("sys/db/rank"), V: Int(0)},
			{E: TempID("3"), A: Ident("test/score"), V: Float(92.0)},
			{E: TempID("3"), A: Ident("sys/db/rank"), V: Int(1)},
			{E: TempID("4"), A: Ident("test/score"), V: Float(98.9)},
			{E: TempID("4"), A: Ident("sys/db/rank"), V: Int(2)},
		},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	assembler := NewAssembler(analyzer, res.Snapshot)
	entity, err := Assemble(assembler, res.TempIDs[TempID("1")], (*Test)(nil))
	assert.NoError(t, err)

	expected := Test{
		Title: "Algebra II",
		// we're pretending order is important here, e.g. tests repeatedly taken over time
		Scores: []float64{95.3, 92.0, 98.9},
	}
	assert.Equal(t, expected, entity)
}
