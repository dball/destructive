// Package index provides for datum indexes implemented on btrees.
package index

import (
	"time"

	"github.com/dball/destructive/internal/iterator"
	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

// IndexType is a type of datum index, e.g. EAV.
type IndexType struct {
	StringLesser Lesser[string]
	IntLesser    Lesser[int64]
	UintLesser   Lesser[uint64]
	FloatLesser  Lesser[float64]
}

type PartialIndex int8

const EA PartialIndex = 1
const E PartialIndex = 2
const AE PartialIndex = 3
const A PartialIndex = 4
const AV PartialIndex = 5
const VA PartialIndex = 6

// EAVIndex is the EAV index type.
var EAVIndex = IndexType{
	StringLesser: LessEAV[string],
	IntLesser:    LessEAV[int64],
	UintLesser:   LessEAV[uint64],
	FloatLesser:  LessEAV[float64],
}

// AEVIndex is the AEV index type.
var AEVIndex = IndexType{
	StringLesser: LessAEV[string],
	IntLesser:    LessAEV[int64],
	UintLesser:   LessAEV[uint64],
	FloatLesser:  LessAEV[float64],
}

// AVEIndex is the AVE index type.
var AVEIndex = IndexType{
	StringLesser: LessAVE[string],
	IntLesser:    LessAVE[int64],
	UintLesser:   LessAVE[uint64],
	FloatLesser:  LessAVE[float64],
}

// VAEIndex is the VAE index type.
var VAEIndex = IndexType{
	StringLesser: LessVAE[string],
	IntLesser:    LessVAE[int64],
	UintLesser:   LessVAE[uint64],
	FloatLesser:  LessVAE[float64],
}

// Index is a sorted set of datums, where the basis for uniqueness is eav. An index
// will retain the extant datum if a new one is inserted for the same eav.
//
// Index instances are safe for concurrent reads, not for concurrent writes.
type Index interface {
	// Find returns true if a datum with the given datum's eav values is present in the indexed set.
	Find(datum Datum) (extant bool)
	// Insert ensures a datum with the given datum's eav values is present in the indexed set. If
	// this returns true, the indexed datum will have the given datum's t value.
	Insert(datum Datum) (extant bool)
	// Delete ensures no datum with the given datum's eav values is present in the indexed set.
	// If this returns true, a datum was deleted in so doing.
	Delete(datum Datum) (extant bool)
	// Select returns an iterator of datums that match the given datum according to the partial
	// index.
	Select(p PartialIndex, datum Datum) (iter *iterator.Iterator[Datum])
	// First returns the first datum matching the partial index, if any.
	First(p PartialIndex, datum Datum) (match Datum, extant bool)
	// Clone returns a copy of the index. Both instances are hereafter safe to change without affecting
	// the other.
	Clone() (clone Index)
}

// CompositeIndex is an index of indexes of the discrete types.
type CompositeIndex struct {
	attrTypes map[ID]ID
	strings   TypedIndex[string]
	ints      TypedIndex[int64]
	uints     TypedIndex[uint64]
	floats    TypedIndex[float64]
}

var _ Index = &CompositeIndex{}

// NewCompositeIndex returns a new composite index of the given degree and index type. This
// creates a btree index for each of the four go scalar types to which the system attribute
// types most naturally serialize.
func NewCompositeIndex(degree int, indexType IndexType, attrTypes map[ID]ID) (composite *CompositeIndex) {
	composite = &CompositeIndex{
		attrTypes: attrTypes,
		strings:   NewBTreeIndex(degree, indexType.StringLesser),
		ints:      NewBTreeIndex(degree, indexType.IntLesser),
		uints:     NewBTreeIndex(degree, indexType.UintLesser),
		floats:    NewBTreeIndex(degree, indexType.FloatLesser),
	}
	return
}

var stringValuer TypeValuer[string] = TypeValuer[string]{
	valuer:   func(v string) (value Value) { return String(v) },
	devaluer: func(value Value) (v string) { return string(value.(String)) },
}
var intValuer TypeValuer[int64] = TypeValuer[int64]{
	valuer:   func(v int64) (value Value) { return Int(v) },
	devaluer: func(value Value) (v int64) { return int64(value.(Int)) },
}
var refValuer TypeValuer[uint64] = TypeValuer[uint64]{
	valuer:   func(v uint64) (value Value) { return Int(v) },
	devaluer: func(value Value) (v uint64) { return uint64(value.(ID)) },
}
var floatValuer TypeValuer[float64] = TypeValuer[float64]{
	valuer:   func(v float64) (value Value) { return Float(v) },
	devaluer: func(value Value) (v float64) { return float64(value.(Float)) },
}
var boolValuer TypeValuer[uint64] = TypeValuer[uint64]{
	valuer: func(v uint64) (value Value) {
		switch v {
		case 0:
			value = Bool(true)
		case 1:
			value = Bool(false)
		}
		return
	},
	devaluer: func(value Value) (v uint64) {
		if bool(value.(Bool)) {
			v = 1
		} else {
			v = 0
		}
		return
	},
}
var instValuer TypeValuer[int64] = TypeValuer[int64]{
	valuer:   func(v int64) (value Value) { return Inst(time.UnixMilli(v)) },
	devaluer: func(value Value) (v int64) { return time.Time(value.(Inst)).UnixMilli() },
}

func (idx *CompositeIndex) Find(datum Datum) (extant bool) {
	switch idx.attrTypes[datum.A] {
	case sys.AttrTypeString:
		extant = idx.strings.Find(TypedDatum[string]{E: datum.E, A: datum.A, V: string(datum.V.(String))})
	case sys.AttrTypeInt:
		extant = idx.ints.Find(TypedDatum[int64]{E: datum.E, A: datum.A, V: int64(datum.V.(Int))})
	case sys.AttrTypeRef:
		extant = idx.uints.Find(TypedDatum[uint64]{E: datum.E, A: datum.A, V: uint64(datum.V.(ID))})
	case sys.AttrTypeFloat:
		extant = idx.floats.Find(TypedDatum[float64]{E: datum.E, A: datum.A, V: float64(datum.V.(Float))})
	case sys.AttrTypeBool:
		if bool(datum.V.(Bool)) {
			extant = idx.uints.Find(TypedDatum[uint64]{E: datum.E, A: datum.A, V: 1})
		} else {
			extant = idx.uints.Find(TypedDatum[uint64]{E: datum.E, A: datum.A, V: 0})
		}
	case sys.AttrTypeInst:
		extant = idx.ints.Find(TypedDatum[int64]{E: datum.E, A: datum.A, V: time.Time(datum.V.(Inst)).UnixMilli()})
	}
	return
}

func (idx *CompositeIndex) Insert(datum Datum) (extant bool) {
	switch idx.attrTypes[datum.A] {
	case sys.AttrTypeString:
		extant = idx.strings.Insert(TypedDatum[string]{E: datum.E, A: datum.A, V: string(datum.V.(String)), T: datum.T})
	case sys.AttrTypeInt:
		extant = idx.ints.Insert(TypedDatum[int64]{E: datum.E, A: datum.A, V: int64(datum.V.(Int)), T: datum.T})
	case sys.AttrTypeRef:
		extant = idx.uints.Insert(TypedDatum[uint64]{E: datum.E, A: datum.A, V: uint64(datum.V.(ID)), T: datum.T})
	case sys.AttrTypeFloat:
		extant = idx.floats.Insert(TypedDatum[float64]{E: datum.E, A: datum.A, V: float64(datum.V.(Float)), T: datum.T})
	case sys.AttrTypeBool:
		if bool(datum.V.(Bool)) {
			extant = idx.uints.Insert(TypedDatum[uint64]{E: datum.E, A: datum.A, V: 1, T: datum.T})
		} else {
			extant = idx.uints.Insert(TypedDatum[uint64]{E: datum.E, A: datum.A, V: 0, T: datum.T})
		}
	case sys.AttrTypeInst:
		extant = idx.ints.Insert(TypedDatum[int64]{E: datum.E, A: datum.A, V: time.Time(datum.V.(Inst)).UnixMilli(), T: datum.T})
	}
	return
}

func (idx *CompositeIndex) Delete(datum Datum) (extant bool) {
	switch idx.attrTypes[datum.A] {
	case sys.AttrTypeString:
		extant = idx.strings.Delete(TypedDatum[string]{E: datum.E, A: datum.A, V: string(datum.V.(String))})
	case sys.AttrTypeInt:
		extant = idx.ints.Delete(TypedDatum[int64]{E: datum.E, A: datum.A, V: int64(datum.V.(Int))})
	case sys.AttrTypeRef:
		extant = idx.uints.Delete(TypedDatum[uint64]{E: datum.E, A: datum.A, V: uint64(datum.V.(ID))})
	case sys.AttrTypeFloat:
		extant = idx.floats.Delete(TypedDatum[float64]{E: datum.E, A: datum.A, V: float64(datum.V.(Float))})
	case sys.AttrTypeBool:
		if bool(datum.V.(Bool)) {
			extant = idx.uints.Delete(TypedDatum[uint64]{E: datum.E, A: datum.A, V: 1})
		} else {
			extant = idx.uints.Delete(TypedDatum[uint64]{E: datum.E, A: datum.A, V: 0})
		}
	case sys.AttrTypeInst:
		extant = idx.ints.Delete(TypedDatum[int64]{E: datum.E, A: datum.A, V: time.Time(datum.V.(Inst)).UnixMilli()})
	}
	return
}

type polyTypeIterator struct {
	idx     *CompositeIndex
	strings *iterator.Iterator[Datum]
	ints    *iterator.Iterator[Datum]
	uints   *iterator.Iterator[Datum]
	floats  *iterator.Iterator[Datum]
}

// TODO our iterators could maintain eav sorting if we build sorted peekahead
func (poly *polyTypeIterator) Each(accept iterator.Accept[Datum]) {
	var iter *iterator.Iterator[Datum]
	iter = poly.strings
	for iter.Next() {
		if !accept(iter.Value()) {
			iter.Stop()
			poly.ints.Stop()
			poly.uints.Stop()
			poly.floats.Stop()
		}
	}
	iter = poly.ints
	for iter.Next() {
		datum := iter.Value()
		switch poly.idx.attrTypes[datum.A] {
		case sys.AttrTypeInt:
		case sys.AttrTypeInst:
			datum.V = instValuer.valuer(int64(datum.V.(Int)))
		default:
			panic("index.typed.int.corrupt")
		}
		if !accept(datum) {
			iter.Stop()
			poly.uints.Stop()
			poly.floats.Stop()
		}
	}
	iter = poly.uints
	for iter.Next() {
		datum := iter.Value()
		switch poly.idx.attrTypes[datum.A] {
		case sys.AttrTypeRef:
		case sys.AttrTypeBool:
			datum.V = boolValuer.valuer(uint64(datum.V.(ID)))
		default:
			panic("index.typed.uint.corrupt")
		}
		if !accept(datum) {
			iter.Stop()
			poly.floats.Stop()
		}
	}
	iter = poly.floats
	for iter.Next() {
		if !accept(iter.Value()) {
			iter.Stop()
		}
	}
}

func (idx *CompositeIndex) Select(p PartialIndex, datum Datum) (iter *iterator.Iterator[Datum]) {
	// TODO should idx ensure p is legit for its type? This would just be a cross check against the
	// database misusing its indexes.
	if p == E {
		strings := idx.strings.Select(CompareE[string], stringValuer.valuer, TypedDatum[string]{E: datum.E})
		ints := idx.ints.Select(CompareE[int64], intValuer.valuer, TypedDatum[int64]{E: datum.E})
		uints := idx.uints.Select(CompareE[uint64], refValuer.valuer, TypedDatum[uint64]{E: datum.E})
		floats := idx.floats.Select(CompareE[float64], floatValuer.valuer, TypedDatum[float64]{E: datum.E})
		iter = iterator.BuildIterator[Datum](&polyTypeIterator{idx, strings, ints, uints, floats})
		return
	}
	switch idx.attrTypes[datum.A] {
	case sys.AttrTypeString:
		switch p {
		case EA:
			iter = idx.strings.Select(CompareEA[string], stringValuer.valuer, TypedDatum[string]{E: datum.E, A: datum.A})
		case AE:
			iter = idx.strings.Select(CompareAE[string], stringValuer.valuer, TypedDatum[string]{E: datum.E, A: datum.A})
		case A:
			iter = idx.strings.Select(CompareA[string], stringValuer.valuer, TypedDatum[string]{A: datum.A})
		case AV:
			iter = idx.strings.Select(CompareAV[string], stringValuer.valuer, TypedDatum[string]{A: datum.A, V: stringValuer.devaluer(datum.V)})
		}
	case sys.AttrTypeInt:
		switch p {
		case EA:
			iter = idx.ints.Select(CompareEA[int64], intValuer.valuer, TypedDatum[int64]{E: datum.E, A: datum.A})
		case AE:
			iter = idx.ints.Select(CompareAE[int64], intValuer.valuer, TypedDatum[int64]{E: datum.E, A: datum.A})
		case A:
			iter = idx.ints.Select(CompareA[int64], intValuer.valuer, TypedDatum[int64]{A: datum.A})
		case AV:
			iter = idx.ints.Select(CompareAV[int64], intValuer.valuer, TypedDatum[int64]{A: datum.A, V: intValuer.devaluer(datum.V)})
		}
	case sys.AttrTypeRef:
		switch p {
		case EA:
			iter = idx.uints.Select(CompareEA[uint64], refValuer.valuer, TypedDatum[uint64]{E: datum.E, A: datum.A})
		case AE:
			iter = idx.uints.Select(CompareAE[uint64], refValuer.valuer, TypedDatum[uint64]{E: datum.E, A: datum.A})
		case A:
			iter = idx.uints.Select(CompareA[uint64], refValuer.valuer, TypedDatum[uint64]{A: datum.A})
		case AV:
			iter = idx.uints.Select(CompareAV[uint64], refValuer.valuer, TypedDatum[uint64]{A: datum.A, V: refValuer.devaluer(datum.V)})
		}
	case sys.AttrTypeFloat:
		switch p {
		case EA:
			iter = idx.floats.Select(CompareEA[float64], floatValuer.valuer, TypedDatum[float64]{E: datum.E, A: datum.A})
		case AE:
			iter = idx.floats.Select(CompareAE[float64], floatValuer.valuer, TypedDatum[float64]{E: datum.E, A: datum.A})
		case A:
			iter = idx.floats.Select(CompareA[float64], floatValuer.valuer, TypedDatum[float64]{A: datum.A})
		case AV:
			iter = idx.floats.Select(CompareAV[float64], floatValuer.valuer, TypedDatum[float64]{A: datum.A, V: floatValuer.devaluer(datum.V)})
		}
	case sys.AttrTypeBool:
		switch p {
		case EA:
			iter = idx.uints.Select(CompareEA[uint64], boolValuer.valuer, TypedDatum[uint64]{E: datum.E, A: datum.A})
		case AE:
			iter = idx.uints.Select(CompareAE[uint64], boolValuer.valuer, TypedDatum[uint64]{E: datum.E, A: datum.A})
		case A:
			iter = idx.uints.Select(CompareA[uint64], boolValuer.valuer, TypedDatum[uint64]{A: datum.A})
		case AV:
			iter = idx.uints.Select(CompareAV[uint64], boolValuer.valuer, TypedDatum[uint64]{A: datum.A, V: boolValuer.devaluer(datum.V)})
		}
	case sys.AttrTypeInst:
		switch p {
		case EA:
			iter = idx.ints.Select(CompareEA[int64], instValuer.valuer, TypedDatum[int64]{E: datum.E, A: datum.A})
		case AE:
			iter = idx.ints.Select(CompareAE[int64], instValuer.valuer, TypedDatum[int64]{E: datum.E, A: datum.A})
		case A:
			iter = idx.ints.Select(CompareA[int64], instValuer.valuer, TypedDatum[int64]{A: datum.A})
		case AV:
			iter = idx.ints.Select(CompareAV[int64], instValuer.valuer, TypedDatum[int64]{A: datum.A, V: instValuer.devaluer(datum.V)})
		}
	}
	return
}

func (idx *CompositeIndex) First(p PartialIndex, datum Datum) (match Datum, extant bool) {
	// TODO should idx ensure p is legit for its type? This would just be a cross check against the
	// database misusing its indexes.
	if p == E {
		// TODO a order is quite important here, right, even though we're never going to need this case
		match, extant = idx.strings.First(CompareE[string], stringValuer.valuer, TypedDatum[string]{E: datum.E})
		if extant {
			return
		}
		match, extant = idx.ints.First(CompareE[int64], intValuer.valuer, TypedDatum[int64]{E: datum.E})
		if extant {
			return
		}
		match, extant = idx.uints.First(CompareE[uint64], refValuer.valuer, TypedDatum[uint64]{E: datum.E})
		if extant {
			return
		}
		match, extant = idx.floats.First(CompareE[float64], floatValuer.valuer, TypedDatum[float64]{E: datum.E})
		return
	}
	switch idx.attrTypes[datum.A] {
	case sys.AttrTypeString:
		switch p {
		case EA:
			match, extant = idx.strings.First(CompareEA[string], stringValuer.valuer, TypedDatum[string]{E: datum.E, A: datum.A})
		case AE:
			match, extant = idx.strings.First(CompareAE[string], stringValuer.valuer, TypedDatum[string]{E: datum.E, A: datum.A})
		case A:
			match, extant = idx.strings.First(CompareA[string], stringValuer.valuer, TypedDatum[string]{A: datum.A})
		case AV:
			match, extant = idx.strings.First(CompareAV[string], stringValuer.valuer, TypedDatum[string]{A: datum.A, V: stringValuer.devaluer(datum.V)})
		}
	case sys.AttrTypeInt:
		switch p {
		case EA:
			match, extant = idx.ints.First(CompareEA[int64], intValuer.valuer, TypedDatum[int64]{E: datum.E, A: datum.A})
		case AE:
			match, extant = idx.ints.First(CompareAE[int64], intValuer.valuer, TypedDatum[int64]{E: datum.E, A: datum.A})
		case A:
			match, extant = idx.ints.First(CompareA[int64], intValuer.valuer, TypedDatum[int64]{A: datum.A})
		case AV:
			match, extant = idx.ints.First(CompareAV[int64], intValuer.valuer, TypedDatum[int64]{A: datum.A, V: intValuer.devaluer(datum.V)})
		}
	case sys.AttrTypeRef:
		switch p {
		case EA:
			match, extant = idx.uints.First(CompareEA[uint64], refValuer.valuer, TypedDatum[uint64]{E: datum.E, A: datum.A})
		case AE:
			match, extant = idx.uints.First(CompareAE[uint64], refValuer.valuer, TypedDatum[uint64]{E: datum.E, A: datum.A})
		case A:
			match, extant = idx.uints.First(CompareA[uint64], refValuer.valuer, TypedDatum[uint64]{A: datum.A})
		case AV:
			match, extant = idx.uints.First(CompareAV[uint64], refValuer.valuer, TypedDatum[uint64]{A: datum.A, V: refValuer.devaluer(datum.V)})
		}
	case sys.AttrTypeFloat:
		switch p {
		case EA:
			match, extant = idx.floats.First(CompareEA[float64], floatValuer.valuer, TypedDatum[float64]{E: datum.E, A: datum.A})
		case AE:
			match, extant = idx.floats.First(CompareAE[float64], floatValuer.valuer, TypedDatum[float64]{E: datum.E, A: datum.A})
		case A:
			match, extant = idx.floats.First(CompareA[float64], floatValuer.valuer, TypedDatum[float64]{A: datum.A})
		case AV:
			match, extant = idx.floats.First(CompareAV[float64], floatValuer.valuer, TypedDatum[float64]{A: datum.A, V: floatValuer.devaluer(datum.V)})
		}
	case sys.AttrTypeBool:
		switch p {
		case EA:
			match, extant = idx.uints.First(CompareEA[uint64], boolValuer.valuer, TypedDatum[uint64]{E: datum.E, A: datum.A})
		case AE:
			match, extant = idx.uints.First(CompareAE[uint64], boolValuer.valuer, TypedDatum[uint64]{E: datum.E, A: datum.A})
		case A:
			match, extant = idx.uints.First(CompareA[uint64], boolValuer.valuer, TypedDatum[uint64]{A: datum.A})
		case AV:
			match, extant = idx.uints.First(CompareAV[uint64], boolValuer.valuer, TypedDatum[uint64]{A: datum.A, V: boolValuer.devaluer(datum.V)})
		}
	case sys.AttrTypeInst:
		switch p {
		case EA:
			match, extant = idx.ints.First(CompareEA[int64], instValuer.valuer, TypedDatum[int64]{E: datum.E, A: datum.A})
		case AE:
			match, extant = idx.ints.First(CompareAE[int64], instValuer.valuer, TypedDatum[int64]{E: datum.E, A: datum.A})
		case A:
			match, extant = idx.ints.First(CompareA[int64], instValuer.valuer, TypedDatum[int64]{A: datum.A})
		case AV:
			match, extant = idx.ints.First(CompareAV[int64], instValuer.valuer, TypedDatum[int64]{A: datum.A, V: instValuer.devaluer(datum.V)})
		}
	}
	return
}
func (idx *CompositeIndex) Clone() (clone Index) {
	clone = &CompositeIndex{
		attrTypes: idx.attrTypes,
		strings:   idx.strings.Clone(),
		ints:      idx.ints.Clone(),
		uints:     idx.uints.Clone(),
		floats:    idx.floats.Clone(),
	}
	return
}
