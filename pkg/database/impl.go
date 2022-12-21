package database

import (
	"reflect"

	"github.com/dball/destructive/internal/database"
	"github.com/dball/destructive/internal/structs/assembler"
	"github.com/dball/destructive/internal/structs/models"
	"github.com/dball/destructive/internal/structs/shredder"
	"github.com/dball/destructive/internal/types"
)

type Config struct {
	Degree     int
	AttrsSize  int
	IdentsSize int
}

var defaultConfig Config = Config{
	Degree:     64,
	AttrsSize:  256,
	IdentsSize: 1024,
}

func NewDatabase(config Config) Database {
	degree := config.Degree
	if degree == 0 {
		degree = defaultConfig.Degree
	}
	attrsSize := config.AttrsSize
	if attrsSize == 0 {
		attrsSize = defaultConfig.AttrsSize
	}
	identsSize := config.IdentsSize
	if identsSize == 0 {
		attrsSize = defaultConfig.IdentsSize
	}
	return &localDatabase{db: database.NewIndexDatabase(degree, attrsSize, identsSize)}
}

type localDatabase struct {
	db types.Database
}

var _ Database = (*localDatabase)(nil)

func (db *localDatabase) Read() Snapshot {
	return &localSnapshot{snap: db.db.Read()}
}

func (db *localDatabase) Write(req Request) (res Response) {
	ireq, err := shredder.NewShredder(models.BuildCachingAnalyzer()).Shred(shredder.Document{
		Retractions: req.Retractions,
		Assertions:  req.Assertions,
	})
	if err != nil {
		res.Error = err
		return
	}
	ires := db.db.Write(ireq)
	if ires.Error != nil {
		res.Error = ires.Error
		return
	}
	res.Snapshot = &localSnapshot{snap: ires.Snapshot}
	// TODO how to assign tempids back to the assertions?
	// TODO how to assign txn id to the transaction entity, if any?
	return
}

type localSnapshot struct {
	snap types.Snapshot
}

var _ Snapshot = (*localSnapshot)(nil)

func (snap *localSnapshot) Find(entity any) (found bool) {
	val := reflect.ValueOf(entity)
	if val.Kind() != reflect.Pointer {
		return
	}
	strukt := val.Elem()
	model, err := models.Analyze(strukt.Type())
	if err != nil {
		return
	}
	var id types.ID
	for _, attrField := range model.AttrFields {
		var fieldID types.ID
		if attrField.Ident == types.Ident("sys/db/id") {
			fieldID = types.ID(strukt.Field(attrField.Index).Uint())
		}
		if fieldID == 0 {
			continue
		}
		if id == 0 {
			id = fieldID
		}
		if id != fieldID {
			return
		}
	}
	if id == 0 {
		return
	}
	datums := snap.snap.Select(types.Claim{E: id}).Drain()
	facts := make([]assembler.Fact, 0, len(datums))
	for _, datum := range datums {
		facts = append(facts, assembler.Fact{E: datum.E, A: snap.snap.ResolveAttrIdent(datum.A), V: datum.V})
	}
	ass, err := assembler.NewAssembler(models.BuildCachingAnalyzer(), entity.(*any), facts)
	if err != nil {
		return
	}
	_, err = ass.Next()
	if err != nil {
		return
	}
	found = true
	return
}
