package schemas

import (
	"reflect"
	"testing"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestAnalyzeSimple(t *testing.T) {
	type Person struct {
		ID    uint64  `attr:"sys/db/id"`
		Name  string  `attr:"person/name,identity"`
		Title *string `attr:"person/title"`
	}
	var p *Person

	actual, err := Analyze(reflect.TypeOf(p).Elem())
	assert.NoError(t, err)
	expected := []Claim{
		{E: TempID("1"), A: sys.DbIdent, V: String("person/name")},
		{E: TempID("1"), A: sys.AttrType, V: sys.AttrTypeString},
		{E: TempID("1"), A: sys.AttrUnique, V: sys.AttrUniqueIdentity},
		{E: TempID("2"), A: sys.DbIdent, V: String("person/title")},
		{E: TempID("2"), A: sys.AttrType, V: sys.AttrTypeString},
	}
	assert.Equal(t, expected, actual)
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

	actual, err := Analyze(reflect.TypeOf(p).Elem())
	assert.NoError(t, err)
	expected := []Claim{
		{E: TempID("1"), A: sys.DbIdent, V: String("person/name")},
		{E: TempID("1"), A: sys.AttrType, V: sys.AttrTypeString},
		{E: TempID("2"), A: sys.DbIdent, V: String("person/favorite-book")},
		{E: TempID("2"), A: sys.AttrType, V: sys.AttrTypeRef},
		{E: TempID("3"), A: sys.DbIdent, V: String("person/best-book")},
		{E: TempID("3"), A: sys.AttrType, V: sys.AttrTypeRef},
		{E: TempID("4"), A: sys.DbIdent, V: String("book/title")},
		{E: TempID("4"), A: sys.AttrType, V: sys.AttrTypeString},
	}
	assert.Equal(t, expected, actual)
}
