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
		name string `attr:"person/name,identity"`
		uuid string `attr:"person/uuid,unique,ignoreempty"`
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
				{E: ID(23), A: Ident("person/name"), V: String("Donald")},
				{E: ID(23), A: Ident("person/age"), V: Int(48)},
				{E: ID(23), A: Ident("person/birthdate"), V: Inst(epoch)},
			},
			Retractions: []*Retraction{},
		}
		assert.Equal(t, expected, req)
	})

	t.Run("retract", func(t *testing.T) {
		shredder := NewShredder()
		p := person{id: 23, name: "Donald", age: 48}
		req, err := shredder.Shred(Document{Retractions: []any{p}})
		assert.NoError(t, err)
		expected := Request{
			Claims: []*Claim{},
			Retractions: []*Retraction{
				{
					Constraints: map[IDRef]Void{
						ID(23): {},
						LookupRef{A: Ident("person/name"), V: String("Donald")}: {},
					},
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
				{E: ID(23), A: Ident("person/name"), V: String("Donald")},
				{E: ID(23), A: Ident("person/uuid"), V: String("the-uuid")},
			},
			Retractions: []*Retraction{},
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
			Retractions: []*Retraction{},
		}
		assert.Equal(t, expected, req)
	})

	t.Run("invalid values", func(t *testing.T) {
		shredder := NewShredder()
		_, err := shredder.Shred(Document{Assertions: []any{5}})
		assert.Error(t, err)
	})
}

/*
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

func TestStructs(t *testing.T) {
	type Book struct {
		Title string `attr:"book/title"`
	}

	type Person struct {
		Name     string `attr:"person/name"`
		Favorite Book   `attr:"person/favorite-book"`
	}

	t.Run("value struct field", func(t *testing.T) {
		shredder := NewShredder()
		me := Person{Name: "Donald", Favorite: Book{Title: "Immortality"}}
		actual, err := shredder.Shred(Document{Assertions: []any{me}})
		assert.NoError(t, err)
		expected := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
				{E: TempID("1"), A: Ident("person/favorite-book"), V: TempID("2")},
				{E: TempID("2"), A: Ident("book/title"), V: String("Immortality")},
			},
			TempIDs: map[TempID]map[IDRef]Void{
				TempID("1"): {},
				TempID("2"): {},
			},
		}
		assert.Equal(t, expected, actual)
	})
}

func TestMapFields(t *testing.T) {
	type Book struct {
		Title  string `attr:"book/title"`
		Author string `attr:"book/author"`
	}

	type Person struct {
		Name          string          `attr:"person/name,unique"`
		FavoriteBooks map[string]Book `attr:"person/favorite-books,key=book/title"`
	}

	t.Run("assert", func(t *testing.T) {
		shredder := NewShredder()
		me := Person{Name: "Donald", FavoriteBooks: map[string]Book{
			"Immortality":          {Title: "Immortality", Author: "Milan Kundera"},
			"Parable of the Sower": {Title: "Parable of the Sower", Author: "Octavia Butler"},
		}}
		actual, err := shredder.Shred(Document{Assertions: []any{me}})
		assert.NoError(t, err)
		// The order of map entries is not specified, so we must allow both permutations.
		expected1 := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
				{E: TempID("1"), A: Ident("person/favorite-books"), V: TempID("2")},
				{E: TempID("1"), A: Ident("person/favorite-books"), V: TempID("3")},
				{E: TempID("2"), A: Ident("book/title"), V: String("Immortality")},
				{E: TempID("2"), A: Ident("book/author"), V: String("Milan Kundera")},
				{E: TempID("3"), A: Ident("book/title"), V: String("Parable of the Sower")},
				{E: TempID("3"), A: Ident("book/author"), V: String("Octavia Butler")},
			},
			TempIDs: map[TempID]map[IDRef]Void{
				TempID("1"): {LookupRef{A: Ident("person/name"), V: String("Donald")}: Void{}},
				TempID("2"): {},
				TempID("3"): {},
			},
		}
		expected2 := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
				{E: TempID("1"), A: Ident("person/favorite-books"), V: TempID("2")},
				{E: TempID("1"), A: Ident("person/favorite-books"), V: TempID("3")},
				{E: TempID("2"), A: Ident("book/title"), V: String("Parable of the Sower")},
				{E: TempID("2"), A: Ident("book/author"), V: String("Octavia Butler")},
				{E: TempID("3"), A: Ident("book/title"), V: String("Immortality")},
				{E: TempID("3"), A: Ident("book/author"), V: String("Milan Kundera")},
			},
			TempIDs: map[TempID]map[IDRef]Void{
				TempID("1"): {LookupRef{A: Ident("person/name"), V: String("Donald")}: Void{}},
				TempID("2"): {},
				TempID("3"): {},
			},
		}
		switch {
		case assert.ObjectsAreEqual(expected1, actual):
			assert.Equal(t, expected1, actual)
		case assert.ObjectsAreEqual(expected2, actual):
			assert.Equal(t, expected2, actual)
		default:
			assert.Equal(t, expected1, actual)
		}
	})
}

func TestScalarSliceFields(t *testing.T) {
	type Test struct {
		Title  string    `attr:"test/title"`
		Scores []float64 `attr:"test/scores,value=test/score"`
	}

	t.Run("assert", func(t *testing.T) {
		shredder := NewShredder()
		test := Test{
			Title: "Algebra II",
			// we're pretending order is important here, e.g. tests repeatedly taken over time
			Scores: []float64{95.3, 92.0, 98.9},
		}
		actual, err := shredder.Shred(Document{Assertions: []any{test}})
		assert.NoError(t, err)
		// TODO the thing we may need to add here is a retraction for the slice ref attr values.
		// Either that, or a txn fn that compares ordered list values and computes the minimal
		// set of datum changes to transform one into the other.
		expected := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("test/title"), V: String("Algebra II")},
				{E: TempID("1"), A: Ident("test/scores"), V: TempID("2")},
				{E: TempID("1"), A: Ident("test/scores"), V: TempID("3")},
				{E: TempID("1"), A: Ident("test/scores"), V: TempID("4")},
				{E: TempID("2"), A: Ident("sys/db/rank"), V: Int(0)},
				{E: TempID("2"), A: Ident("test/score"), V: Float(95.3)},
				{E: TempID("3"), A: Ident("sys/db/rank"), V: Int(1)},
				{E: TempID("3"), A: Ident("test/score"), V: Float(92.0)},
				{E: TempID("4"), A: Ident("sys/db/rank"), V: Int(2)},
				{E: TempID("4"), A: Ident("test/score"), V: Float(98.9)},
			},
			TempIDs: map[TempID]map[IDRef]Void{
				TempID("1"): {},
				TempID("2"): {},
				TempID("3"): {},
				TempID("4"): {},
			},
		}
		assert.Equal(t, expected, actual)
	})
}

func TestStructSliceFields(t *testing.T) {
	// Note this will produce datums equivalent to the scalar example above, the only difference being
	// that the Run structs have room to record/represent other data.

	type Run struct {
		Score float64 `attr:"test/score"`
	}

	type Test struct {
		Title string `attr:"test/title"`
		Runs  []Run  `attr:"test/scores"`
	}

	t.Run("assert", func(t *testing.T) {
		shredder := NewShredder()
		test := Test{
			Title: "Algebra II",
			Runs: []Run{
				{Score: 95.3},
				{Score: 92.0},
				{Score: 98.9},
			},
		}
		actual, err := shredder.Shred(Document{Assertions: []any{test}})
		assert.NoError(t, err)
		// TODO the thing we may need to add here is a retraction for the slice ref attr values.
		// Either that, or a txn fn that compares ordered list values and computes the minimal
		// set of datum changes to transform one into the other.
		expected := Request{
			Claims: []*Claim{
				{E: TempID("1"), A: Ident("test/title"), V: String("Algebra II")},
				{E: TempID("1"), A: Ident("test/scores"), V: TempID("2")},
				{E: TempID("1"), A: Ident("test/scores"), V: TempID("3")},
				{E: TempID("1"), A: Ident("test/scores"), V: TempID("4")},
				{E: TempID("2"), A: Ident("sys/db/rank"), V: Int(0)},
				{E: TempID("2"), A: Ident("test/score"), V: Float(95.3)},
				{E: TempID("3"), A: Ident("sys/db/rank"), V: Int(1)},
				{E: TempID("3"), A: Ident("test/score"), V: Float(92.0)},
				{E: TempID("4"), A: Ident("sys/db/rank"), V: Int(2)},
				{E: TempID("4"), A: Ident("test/score"), V: Float(98.9)},
			},
			TempIDs: map[TempID]map[IDRef]Void{
				TempID("1"): {},
				TempID("2"): {},
				TempID("3"): {},
				TempID("4"): {},
			},
		}
		assert.Equal(t, expected, actual)
	})
}
*/
