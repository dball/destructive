package sys

import (
	"strings"
	"time"

	. "github.com/dball/destructive/internal/types"
)

const (
	DbId                 = "sys/db/id"
	DbIdent              = ID(1)
	AttrType             = ID(2)
	AttrUnique           = ID(3)
	AttrCardinality      = ID(4)
	Tx                   = ID(5)
	TxAt                 = ID(6)
	AttrUniqueIdentity   = ID(7)
	AttrUniqueValue      = ID(8)
	AttrCardinalityOne   = ID(9)
	AttrCardinalityMany  = ID(10)
	AttrTypeRef          = ID(11)
	AttrTypeString       = ID(12)
	AttrTypeInt          = ID(13)
	AttrTypeBool         = ID(14)
	AttrTypeInst         = ID(15)
	AttrTypeFloat        = ID(16)
	AttrRefType          = ID(17)
	AttrRefTypeDependent = ID(18)
	DbRank               = ID(19)
	FirstUserID          = ID(0x100000)
)

var epoch time.Time

var Datums []Datum = []Datum{
	{E: DbIdent, A: DbIdent, V: String("sys/db/ident"), T: Tx},
	{E: DbIdent, A: AttrType, V: AttrTypeString, T: Tx},
	{E: DbIdent, A: AttrUnique, V: AttrUniqueIdentity, T: Tx},
	{E: AttrUnique, A: DbIdent, V: String("sys/attr/unique"), T: Tx},
	{E: AttrUnique, A: AttrType, V: AttrTypeRef, T: Tx},
	{E: AttrUniqueIdentity, A: DbIdent, V: String("sys/attr/unique/identity"), T: Tx},
	{E: AttrUniqueValue, A: DbIdent, V: String("sys/attr/unique/value"), T: Tx},
	{E: TxAt, A: DbIdent, V: String("sys/tx/at"), T: Tx},
	{E: TxAt, A: AttrType, V: AttrTypeInst, T: Tx},
	{E: AttrType, A: DbIdent, V: String("sys/attr/type"), T: Tx},
	{E: AttrType, A: AttrType, V: AttrTypeRef, T: Tx},
	{E: AttrTypeRef, A: DbIdent, V: String("sys/attr/type/ref"), T: Tx},
	{E: AttrTypeString, A: DbIdent, V: String("sys/attr/type/string"), T: Tx},
	{E: AttrTypeInst, A: DbIdent, V: String("sys/attr/type/inst"), T: Tx},
	{E: AttrTypeInt, A: DbIdent, V: String("sys/attr/type/int"), T: Tx},
	{E: AttrTypeBool, A: DbIdent, V: String("sys/attr/type/bool"), T: Tx},
	{E: AttrTypeFloat, A: DbIdent, V: String("sys/attr/type/float"), T: Tx},
	{E: AttrCardinality, A: DbIdent, V: String("sys/attr/cardinality"), T: Tx},
	{E: AttrCardinality, A: AttrType, V: AttrTypeRef, T: Tx},
	{E: AttrCardinalityOne, A: DbIdent, V: String("sys/attr/cardinality/one"), T: Tx},
	{E: AttrCardinalityMany, A: DbIdent, V: String("sys/attr/cardinality/many"), T: Tx},
	{E: AttrRefType, A: DbIdent, V: String("sys/attr/ref/type"), T: Tx},
	{E: AttrRefType, A: AttrType, V: AttrTypeRef, T: Tx},
	{E: AttrRefTypeDependent, A: DbIdent, V: String("sys/attr/ref/type/dependent"), T: Tx},
	{E: DbRank, A: DbIdent, V: String("sys/db/rank"), T: Tx},
	{E: DbRank, A: AttrType, V: AttrTypeInt, T: Tx},
	{E: Tx, A: TxAt, V: Inst(epoch), T: Tx},
}

// Attrs could be computed from Datums but this is smaller than the reducer code.
var Attrs map[ID]Attr = map[ID]Attr{
	DbIdent:         {ID: DbIdent, Type: AttrTypeString, Unique: AttrUniqueIdentity, Ident: Ident("sys/db/ident")},
	AttrUnique:      {ID: AttrUnique, Type: AttrTypeRef, Ident: Ident("sys/attr/unique")},
	AttrType:        {ID: AttrType, Type: AttrTypeRef, Ident: Ident("sys/attr/type")},
	AttrCardinality: {ID: AttrCardinality, Type: AttrTypeRef, Ident: Ident("sys/attr/cardinality")},
	AttrRefType:     {ID: AttrRefType, Type: AttrTypeRef, Ident: Ident("sys/attr/ref/type")},
	TxAt:            {ID: TxAt, Type: AttrTypeInst, Ident: Ident("sys/tx/at")},
	DbRank:          {ID: DbRank, Type: AttrTypeInt, Ident: Ident("sys/db/rank")},
}

// Idents could also be computed from Datums.
var Idents map[Ident]ID = map[Ident]ID{
	Ident("sys/db/ident"):                DbIdent,
	Ident("sys/attr/unique"):             AttrUnique,
	Ident("sys/tx/at"):                   TxAt,
	Ident("sys/attr/type"):               AttrType,
	Ident("sys/attr/type/ref"):           AttrTypeRef,
	Ident("sys/attr/type/string"):        AttrTypeString,
	Ident("sys/attr/type/inst"):          AttrTypeInst,
	Ident("sys/attr/type/int"):           AttrTypeInt,
	Ident("sys/attr/type/bool"):          AttrTypeBool,
	Ident("sys/attr/type/float"):         AttrTypeFloat,
	Ident("sys/attr/cardinality"):        AttrCardinality,
	Ident("sys/attr/cardinality/one"):    AttrCardinalityOne,
	Ident("sys/attr/cardinality/many"):   AttrCardinalityMany,
	Ident("sys/attr/ref/type"):           AttrRefType,
	Ident("sys/attr/ref/type/dependent"): AttrRefTypeDependent,
	Ident("sys/db/rank"):                 DbRank,
}

func ValidValue(typ ID, value Value) (ok bool) {
	switch typ {
	case AttrTypeRef:
		_, ok = value.(ID)
	case AttrTypeString:
		_, ok = value.(String)
	case AttrTypeInt:
		_, ok = value.(Int)
	case AttrTypeBool:
		_, ok = value.(Bool)
	case AttrTypeInst:
		_, ok = value.(Inst)
	case AttrTypeFloat:
		_, ok = value.(Float)
	}
	return
}

func ValidUnique(id ID) bool {
	switch id {
	case AttrUniqueIdentity:
	case AttrUniqueValue:
	default:
		return false
	}
	return true
}

func ValidAttrType(id ID) bool {
	switch id {
	case AttrTypeRef:
	case AttrTypeString:
	case AttrTypeInt:
	case AttrTypeBool:
	case AttrTypeInst:
	case AttrTypeFloat:
	default:
		return false
	}
	return true
}

func ValidAttrCardinality(id ID) bool {
	switch id {
	case AttrCardinalityOne:
	case AttrCardinalityMany:
	default:
		return false
	}
	return true
}

func ValidAttrRefType(id ID) bool {
	switch id {
	case AttrRefTypeDependent:
		return true
	}
	return false
}

func ValidUserIdent(value String) bool {
	return !strings.HasPrefix(string(value), "sys/")
}
