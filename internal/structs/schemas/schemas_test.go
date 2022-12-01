package schemas

import (
	"reflect"
	"testing"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func Test_AnalyzeSimple(t *testing.T) {
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
