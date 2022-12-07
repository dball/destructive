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

func stringUnwrap(typed TypedDatum[string]) (datum Datum) {
	datum = Datum{E: typed.E, A: typed.A, V: String(any(typed.V).(string)), T: typed.T}
	return
}

type Index interface {
	Find(datum Datum) (extant bool)
	Insert(datum Datum) (extant bool)
	Delete(datum Datum) (extant bool)
	Select(p PartialIndex, datum Datum) (iter *iterator.Iterator[Datum])
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

// NewCompositeIndex returns a new composite index of the given degree and type.
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

func (idx *CompositeIndex) Select(p PartialIndex, datum Datum) (iter *iterator.Iterator[Datum]) {
	// TODO should idx ensure p is legit for its type? This would just be a cross check against the
	// database misusing its indexes.

	if p == E {
		strings := idx.strings.Select(CompareE[string], stringUnwrap, TypedDatum[string]{E: datum.E})
		// TODO other typed index cases
		// TODO our iterators could maintain eav sorting if build one that peeks ahead
		iter = iterator.Iterators(strings)
		return
	}
	switch idx.attrTypes[datum.A] {
	case sys.AttrTypeString:
		switch p {
		case EA:
			iter = idx.strings.Select(CompareEA[string], stringUnwrap, TypedDatum[string]{E: datum.E, A: datum.A})
		case AE:
			iter = idx.strings.Select(CompareAE[string], stringUnwrap, TypedDatum[string]{E: datum.E, A: datum.A})
		case A:
			iter = idx.strings.Select(CompareA[string], stringUnwrap, TypedDatum[string]{A: datum.A})
		case AV:
			iter = idx.strings.Select(CompareAV[string], stringUnwrap, TypedDatum[string]{A: datum.A, V: string(datum.V.(String))})
		}
		// TODO all the cases
	case sys.AttrTypeInt:
	case sys.AttrTypeRef:
	case sys.AttrTypeFloat:
	case sys.AttrTypeBool:
		if bool(datum.V.(Bool)) {
		} else {
		}
	case sys.AttrTypeInst:
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
