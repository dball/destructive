package database

import (
	"reflect"

	"github.com/dball/destructive/internal/database"
	"github.com/dball/destructive/internal/structs/models"
	"github.com/dball/destructive/internal/structs/schemas"
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
	return &localDatabase{
		db:       database.NewIndexDatabase(degree, attrsSize, identsSize),
		analyzer: models.BuildCachingAnalyzer(),
	}
}

type localDatabase struct {
	db       types.Database
	analyzer models.Analyzer
}

var _ Database = (*localDatabase)(nil)

func (db *localDatabase) Read() *Snapshot {
	return &Snapshot{
		snap:     db.db.Read(),
		analyzer: db.analyzer,
	}
}

func (db *localDatabase) Write(req Request) (res Response) {
	for _, assertion := range req.Assertions {
		// TODO we should keep a registry of types whose attrs
		// have been asserted in the database already.
		claims, err := schemas.Analyze(reflect.TypeOf(assertion))
		if err != nil {
			res.Error = err
			return
		}
		// TODO holy non-atomicity, batman. Probably we should accumulate
		// all claims and at least write them all in one batch?
		// For the subsequent data claims, perhaps the internal db write
		// should accommodate a separate "ddl" claims form and handle
		// a total reversion if the data claims have an error?
		ires := db.db.Write(types.Request{Claims: claims})
		if ires.Error != nil {
			res.Error = ires.Error
			return
		}
	}
	ireq, err := shredder.NewShredder(db.analyzer).Shred(shredder.Document{
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
	res.Snap = &Snapshot{
		snap:     ires.Snapshot,
		analyzer: db.analyzer,
	}
	// TODO how to assign tempids back to the assertions?
	// TODO how to assign txn id to the transaction entity, if any?
	return
}
