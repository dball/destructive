package database

import (
	"testing"

	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestWriteSimple(t *testing.T) {
	db := NewIndexDatabase(32, 4)
	req := Request{
		Claims: []*Claim{
			{E: TempID("1"), A: sys.DbIdent, V: String("test/ident")},
		},
		TempIDs: map[TempID]map[IDRef]Void{},
	}
	res := db.Write(req)
	assert.NoError(t, res.Error)
	assert.Positive(t, res.ID)
	assert.Equal(t, map[TempID]ID{TempID("1"): sys.FirstUserID + 1}, res.NewIDs)
	assert.NotNil(t, res.Snapshot)
}
