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

	t.Run("simple entity", func(t *testing.T) {
		db := database.NewIndexDatabase(32, 64, 64)
		analyzer := models.BuildCachingAnalyzer()
		claims, err := schemas.Analyze(reflect.TypeOf(Person{}))
		assert.NoError(t, err)
		res := db.Write(Request{Claims: claims})
		assert.NoError(t, res.Error)
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
		res = db.Write(req)
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
	})
}
