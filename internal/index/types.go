package index

// IndexType is a type of datum index, e.g. EAV.
type IndexType struct {
	StringLesser Lesser[string]
	IntLesser    Lesser[int64]
	UintLesser   Lesser[uint64]
	FloatLesser  Lesser[float64]
}

// EAVIndex is the EAV index type.
var EAVIndex = IndexType{
	StringLesser: LessEAV[string],
	IntLesser:    LessEAV[int64],
	UintLesser:   LessEAV[uint64],
	FloatLesser:  LessEAV[float64],
}

// CompositeIndex is an index of indexes of the discrete types.
type CompositeIndex struct {
	strings Index[string]
	ints    Index[int64]
	uints   Index[uint64]
	floats  Index[float64]
}

// NewCompositeIndex returns a new composite index of the given degree fand type.
func NewCompositeIndex(degree int, indexType IndexType) (composite *CompositeIndex) {
	composite = &CompositeIndex{
		strings: NewBTreeIndex(degree, indexType.StringLesser),
		ints:    NewBTreeIndex(degree, indexType.IntLesser),
		uints:   NewBTreeIndex(degree, indexType.UintLesser),
		floats:  NewBTreeIndex(degree, indexType.FloatLesser),
	}
	return
}
