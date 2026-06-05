# Architecture

## Components

* The transactor records user transactions.
* The database resolves user queries.
* The shredder transforms entity and schema structs into transactions.
* The assembler resolves and populates entity structs from the database.

## Testing

### Organization

* The fundamental properties of the system should be tested in terms of datums and claims, not entity structs.
* The entity struct features should be tested in terms of datums and claims.

### Fundamental Properties

* Every attribute entity has an ident and a type.
* The transactor rejects claims that
  * assert different types for attribute entities.
  * assign "sys/*" attribute values.
  * with values inconsistent with their types, including all nil values.
  * with multiple values for cardinality one attributes.
  * with value uniqueness attributes inconsistent with the database.
* The transactor resolves tempids values for identity uniqueness attributes to existing entities if present, and to new entity ids otherwise.

### Entity Struct Properties

* The shredder rejects entity structs with
  * unregistered attributes.
  * field types inconsistent with their attribute types or cardinalities.
  * entity structs 
* The shredder rejects map fields without keys.
* The shredder ignores fields on entity structs that lack attribute tags.
* The shredder accepts fields for maps and slices that are values or pointers, and treats them equivalently. Empty or nil collections are assumed to retract all such datums.
* The shredder accepts fields for scalar types that are values or pointers. Nil pointer values are assumed to retract any such datum.
* The shredder accepts entity struct values or pointers for ref attributes, and traverses the entity struct graph recursively. Entity structs that have the same total value as an entity struct that has already been shredded are skipped. The entity struct graph is assumed to be stable while being shredded.
* The shredder produces claims for the schema structs consisting of the attributes declared on the fields, using the same graph traversal logic as for entity structs.
* Entity struct slice values will be recorded and presented in order by introducing the system-managed `db/sys/rank` attribute. Sliced collections are assumed to be complete when recording, and will therefore retract any extant entries other than those given in the record.
* Slices of scalar values are allowed when the required value tag directive is present. The scalar values in such slices are recorded as ref entities with the actual scalar value stored on the value tag directive value.

## Known Gaps

* Snapshots clone their `idents` and `attrs` maps eagerly rather than sharing them copy-on-write, so a read is more expensive than the btree clones alone require. See the comment at `internal/database/database.go` `read()`.
* The `vae` (back-reference) index is a full `CompositeIndex` even though it only ever stores ref values, which are uint-typed; a uint-only index would suffice. See the `TODO` at `internal/database/database.go` where `vae` is constructed.

## Planned Extensions

These features are anticipated but unimplemented. Each is expected to be additive — extending caches, the retraction expansion, and the query surface — without changing the datum model, the four-index structure, the transactor's clone-and-swap, or the snapshot interface.

* Dependent references. The `Retraction` doc's recursive semantics apply only to dependent references: retracting an entity should also retract the entities it owns through dependent-ref attributes. The vocabulary already exists (`sys/attr/ref/type`, `sys/attr/ref/type/dependent`, the `Attr.RefType` field, and `sys.ValidAttrRefType`); what is missing is enforcement — an `attrRefTypes` cache populated alongside the other attribute caches in the write path, and transitive (cycle-aware) expansion of the retracted entity set in `Write`. Ownership is expected to be exclusive (a dependent entity belongs to a single owner), which avoids reference counting. Plain references are correctly never cascaded; see `TestRetractDoesNotCascadeToReferences`.
* Reverse-reference queries. The `vae` index already maintains, for every ref attribute, a value-attribute-entity ordering — so the data to answer "which entities reference X" is already indexed and maintained on write, retract, and cardinality replacement. Exposing it needs only a query path: a `V`/`VA` partial over the uint sub-tree (the `VA` partial is already reserved) and snapshot routing. Such queries are already answerable today by value-filtering an attribute scan; `vae` makes them efficient without changing results.