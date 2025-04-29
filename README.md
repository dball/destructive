# Destructive

Destructive is an experiment in indexed data storage in Golang, specifically focused on storing and querying structs.

## Motivations

* I often have large numbers of structs in a collection that I need to search in various ways.
* I often need to efficiently apply changes to the collection without affecting current readers.
* I often need to convert from one type of struct to another with very similar or identical data.
* I am convinced the datum rdf model is widely applicable and underused.

## Constraints

* I only care about local storage at this time. Durable data storage and interchange are goals, but not immediate.
* I care most about correct behavior, then API usability and stability, then performance, then memory efficiency.
* I do not care about being able to go back in history at this time. The data model readily supports it, but it would require a more sophisticated index to be practical.

## TODO

* [x] Rewrite iterator with generics
* [x] Rewrite types and system datums
* [x] Write robust shredder to extract datums from (graphs of) structs
  * [ ] map keys that are not reflected in their values
  * [ ] retract (av?) extant map and slice datums when asserting
  * [ ] declare dependent refs
  * [x] declare schema
* [x] Write robust assembler
* [x] Rewrite b-tree indexes with datum generics
* [ ] Rewrite database/connect/view atop indexes
* [ ] Experiment with struct query objects

## Acknowledgements

This library is an implementation of many of the ideas and features of the Datomic databases, albeit with no durability beyond the process runtime and no transaction history. In this respect, it is significantly also inspired by the Datascript library.

I wrote the low-level pieces last year in Constructive, but got bored before I fleshed out the struct reflection bits.

* [Datomic](https://www.datomic.com/)
* [Datascript](https://github.com/tonsky/datascript)
* [Constructive](https://github.com/dball/constructive)

Destructive uses slightly different system idents than either, preferring a path hierachy.

## Theory

The datum model is an extension of the RDF model. There are four components to a datum:

### Entity

An entity is a thing with an identity. Its characteristics may change over time, but its identity persists. I am an entity.

### Attribute

An attribute is a property of an entity. Attributes have global identities and are themselves entities. "Person's given name" is an attribute.

### Value

A value is an observation, a claim, a fact, a reading. "Donald" is a value which happens to be my current given name.

### Transaction

A transaction is an entity that asserts a set of datums are true at a point in time. When I register an account, my name, credentials, etc. are collectively recorded in a transaction. The transaction necessarily records the time but may also include other audit details like the IP address of the host from which I created the account.

----

I contend this data model constitutes the simplest possible fundamental data model for general use. Simplifying by dropping the transaction component results in a system which has no general treatment of data attribution and is a huge problem for the industry.

This is not a great data model for representing observations about things that lack durable identities or where time and attribution are not important features. It is particularly valuable when working with data combined from diverse sources, where being able to track the provenance of data consistently is important. Joining
datasets either by sharing attributes or asserting a specific relationship between attributes can be readily expressed and often straightforwardly implemented.

## Attributes

Attributes are entities that have at least two system attributes, ident and type, and are governed by others. The system attribute values, once asserted, may neither be asserted anew with new values nor retracted.

### sys/db/ident

An system ident uniquely identifies a datum by name, e.g. `person/name` or, indeed, `sys/db/ident`. Attributes are almost always referred to by their idents. Not all idents are attributes, only those with types. Idents are also the idiomatic way to represent enumerations, and have many uses beyond. The system reserves the `sys`
root, rejecting claims for such idents or about the entities to which they may refer. Users may use the remainder of the space as they see fit, though they're
recommended to use paths for consistency.

### sys/attr/type

This identifies the type of value to which the attribute refers, one of:

* `sys/attr/type/string`
* `sys/attr/type/inst` a moment in time, recorded with millisecond precision
* `sys/attr/type/int`
* `sys/attr/type/float`
* `sys/attr/type/ref` a reference to an entity
* `sys/attr/type/bool`

Nil is not a valid value for any type. The absence of a value is represented by the absence of the datum. An affirmation of a value's absence should be represented by another attribute if the zero value is valid in the use domain.

### sys/attr/cardinality

This specifies the number of values to which the attribute may refer. The valid cardinality values are:

* `sys/attr/cardinality/one`
* `sys/attr/cardinality/many`

Cardinality one, a scalar, is assumed in the absence of a cardinality attribute. Cardinality many uses set semantics.

### sys/attr/unique

This specifies that the attribute's value is unique in the database, only one entity may assert it. It has two values:

* `sys/attr/unique/identity`
* `sys/attr/unique/value`

Both enforce the uniqueness constraint. The only difference is that when asserting claims, if a tempid is used in a claim for this attribute, and an entity already asserts the claimed value, the tempid will resolve to the extant entity for identity uniqueness. By contrast, a value uniqueness attribute will cause the claim to be rejected.

### sys/db/rank

This is a system-managed attribute assigned to the entities that comprise ordered lists. These
are integers, must have unique values in their container entity, and provide an ordering for the
referent entities.

## Structs

Structs are the dominant choice for modeling domain data in Golang, therefore to be useful, this library must provide excellent integration with them.

Structs may be given to the database to record, and may be requested of the database as populated results. Both uses are governed by struct tags.

Each struct represents an entity in the database. Fields on the structs that map to attributes are declared using the `attr` struct tag with a value of the attribute's ident:

```go
type Person struct {
  Name string `attr:"person/name"`
}
```

The ident may be followed by one or more tag directives separated by commas.

Entities do not have strong associations with the structs from which they may have been recorded. It is perfectly reasonable to load an entity into a different type of struct than that from which it was recorded.

### Types

The Golang field type governs the type of the attribute. Value and pointer types of this list are supported:

* `string`
* `int` (this will probably become `int64` for clarity)
* `bool`
* `float64`
* `time.Time`

The system id may also be recorded in a `uint64` field with a `sys/db/id` ident.

When a scalar field has a pointer type, `nil` is taken to indicate the affirmative absence of a value for the attribute. When it has a value type, the empty value is treated like any other unless
the `ignoreempty` directive is present, in which case empty values are treated like `nil` values in
the pointer case.

### References and Collections

In addition to scalar values, fields may contain structs, pointers to structs, slices of scalars or structs, and maps of structs indexed by entity values.

#### Struct references

Struct references correspond to ref attributes. When recording structs, references to structs with
attribute tags are following recursively, with each struct pointer being processed once only. It is
recommended therefore when recording graphs that admit cycles or duplicates to use pointers. For example:

```go
type Person struct {
  Name string `attr:"person/name"`
  BestFriend *Person `attr:"person/best-friend"`
}
```

#### Slices

Slices of structs are fairly straightforward:

```go
type Person struct {
  Name string `attr:"person/name"`
  Pets []Pet `attr:"person/pets"`
}

type Pet struct {
  Name string `attr:"pet/name"`
}
```

Slices will be recorded and presented in order by introducing the system-managed `db/sys/rank` attribute. Sliced collections are assumed to be complete when recording, and will therefore
retract any extant entries other than those given in the record.

Since the parent's ranking recorded on the child entity, the ref relationship must be dependent, and when recording, the child entities may not have any unique identifiers.

Slices of scalars are also allowed:

```go
type Person struct {
  Name string `attr:"person/name"`
  Measurements []float64 `person/measurement,value=test/measurement`
}
```

but require a `value=` tag directive to indicate the attribute of the scalar values. Slices of
scalars may not have pointer values.

#### Maps

Fields may contain maps of structs indexed by values that may or may not be present in the referent structs. For example:

```go
type Person struct {
  Name string `attr:"person/name"`
  FavoriteBooks map[string]Book `attr:"person/favorite-books,key=book/title"`
}

type Book struct {
  Name string `attr:"book/title"`
  Author string `attr:"book/author"`
}
```

If the map key is present in the value struct, they must be consistent when recording.

Map values may be structs or pointers to structs.

### Recording

#### Identities

Fields may declare they correspond to `unique` or `identity` attributes by including those as directives. They are mutually inconsistent, only one way be given. Structs may have multiple such attributes, and may also have a database id field.

When recording an entity, if none of these values resolve to an extant entity, a new entity will be created. If they resolve to a single entity, subject to the constraints of the `unique` attributes, the given attribute values are recorded. Except as noted above for sorted collections, any existing attributes not reflected in the recorded struct are left untouched.

If the identifiers resolve to multiple extant entities, or if the struct does not have an id and any `unique` attributes resolve to an extant entity, the recording will fail.

Structs are currently retracted in full, that is to say, all attributes of the resolved entity
are retracted.
