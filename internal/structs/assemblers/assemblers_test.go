package assemblers

import (
	"testing"
	"time"

	"github.com/dball/destructive/internal/database"
	"github.com/dball/destructive/internal/structs/models"
	"github.com/dball/destructive/internal/sys"
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
		//claims, err := schemas.Analyze(reflect.TypeOf((*Person)(nil)))
		//db.Write(Request{Claims: claims})
		database.Declare(db,
			Attr{Ident: "person/name", Type: sys.AttrTypeString},
			Attr{Ident: "person/title", Type: sys.AttrTypeString},
		)
		req := Request{
			Claims: []Claim{
				{E: TempID("1"), A: Ident("person/name"), V: String("Donald")},
				{E: TempID("1"), A: Ident("person/title"), V: String("Citizen")},
			},
		}
		res := db.Write(req)
		id := res.TempIDs[TempID("1")]
		assert.Positive(t, id)
		assembler := NewAssembler(analyzer, res.Snapshot)
		entity, err := Assemble(assembler, id, (*Person)(nil))
		assert.NoError(t, err)
		title := "Citizen"
		expected := Person{ID: uint64(id), Name: "Donald", Title: &title}
		assert.Equal(t, expected, entity)
	})
}
