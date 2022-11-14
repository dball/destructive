package types

// Attr is a convenient represention of the attributes of an attribute.
type Attr struct {
	// ID is the internal identifier of an attribute.
	ID ID `attr:"sys/db/id"`
	// Ident is the public identifier of an attribute.
	Ident Ident `attr:"sys/db/ident"`
	// TODO might it be useful to have a type for an Ident referred to by
	// an ID for unique attrs?
	// Type specifies the type of values to which the attribute refers.
	Type ID `attr:"sys/attr/type"`
	// Cardinality specifies the number of values an attribute may have on a given entity.
	Cardinality ID `attr:"sys/attr/cardinality"`
	// Unique specifies the uniqueness of the attribute's value.
	Unique ID `attr:"sys/db/unique"`
	// RefType specifies the reference type, if this is a special reference type.
	RefType ID `attr:"sys/attr/ref/type"`
}
