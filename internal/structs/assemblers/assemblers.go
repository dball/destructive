// Package assemblers provides for the construction of structs from sets of datums.
package assemblers

import (
	"reflect"
	"time"

	"github.com/dball/destructive/internal/structs/models"
	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
	"github.com/dball/destructive/pkg/database"
)

type mapAwaitingEntry struct {
	mapKey         Ident
	m              reflect.Value
	pointer        reflect.Value
	mapHasPointers bool
}

type sliceAwaitingEntry struct {
	collValue Ident
	slice     reflect.Value
	pointer   reflect.Value
}

// TODO an assembler can only have a single instance type per id, which is not a constraint imposed
// by the database. Should we reify this more formally?
type assembler struct {
	// analyzer converts types to struct models
	analyzer models.Analyzer
	// snapshot holds the actual datums
	snapshot Snapshot
	// instances are pointers to fully realized entity instances
	instances map[ID]reflect.Value
	// pointers are pointers to all of the at least partially realized entities allocated by the assembler
	pointers map[ID]reflect.Value
	// unprocessed are (not nil) pointers to unrealized entities
	unprocessed map[ID]reflect.Value
	// mapsAwaitingEntries are maps in entity struct fields awaiting referent entities to be realized
	mapsAwaitingEntries map[ID][]mapAwaitingEntry
	// slicesAwaitingEntries are slices in entity struct fields awaiting referent entities to be realized
	slicesAwaitingEntries map[ID][]sliceAwaitingEntry
}

func NewAssembler(analyzer models.Analyzer, snapshot database.Snapshot) (a *assembler) {
	a = &assembler{
		analyzer:              analyzer,
		instances:             map[ID]reflect.Value{},
		pointers:              map[ID]reflect.Value{},
		unprocessed:           map[ID]reflect.Value{},
		mapsAwaitingEntries:   map[ID][]mapAwaitingEntry{},
		slicesAwaitingEntries: map[ID][]sliceAwaitingEntry{},
	}
	return
}

func (a *assembler) allocate(id ID, pointerType reflect.Type) (ptr reflect.Value) {
	// allocate the new pointer
	pp := reflect.New(pointerType)
	ptr = pp.Elem()
	// allocate the new struct
	entity := reflect.New(ptr.Type().Elem())
	// store the new struct in the new pointer
	ptr.Set(entity)
	// store the pointer in the unrealized entities map
	a.unprocessed[id] = ptr
	a.pointers[id] = ptr
	return
}

func (a *assembler) assembleAll() (err error) {
	for {
		if len(a.unprocessed) == 0 {
			break
		}
		var id ID
		var ptr reflect.Value
		// TODO this should ideally be the first value in id order, so, again, sorted map
		for k, v := range a.unprocessed {
			id = k
			ptr = v
			delete(a.unprocessed, k)
			break
		}
		err = a.assemble(id, ptr)
		if err != nil {
			return
		}
	}
	return
}

func (a *assembler) assemble(id ID, ptr reflect.Value) (err error) {
	value := ptr.Elem()

	model, modelErr := a.analyzer.Analyze(value.Type())
	if modelErr != nil {
		err = modelErr
		return
	}

	// We know the A's in which we're interested, so we could be more selective (heh) here
	// if we had reason to believe the snapshot E had many more A's.
	iter := a.snapshot.Select(Claim{E: id})
	foundAny := false
	if foundAny {
		attr, ok := model.Attr(Ident(sys.DbId))
		if ok {
			field := value.Field(attr.Index)
			field.SetUint(uint64(id))
		}
	}

	for iter.Next() {
		foundAny = true
		datum := iter.Value()
		attr, ok := model.Attr(a.snapshot.ResolveAttrIdent(datum.A))
		if !ok {
			// Here's where we could be accumulating stats of attr hit rates for e types, sort of.
			continue
		}
		field := value.Field(attr.Index)
		if attr.IsPointer() {
			// TODO who owns the vs anyway? If they're not copied at some point,
			// exposing value pointers opens the door to database corruption.
			switch v := datum.V.(type) {
			case String:
				// TODO does this count as a copy for the purpose of ensuring the
				// outer pointer doesn't change the Fact value?
				fv := string(v)
				field.Set(reflect.ValueOf(&fv))
			case Int:
				fv := int(v)
				field.Set(reflect.ValueOf(&fv))
			case Bool:
				fv := bool(v)
				field.Set(reflect.ValueOf(&fv))
			case Float:
				fv := float64(v)
				field.Set(reflect.ValueOf(&fv))
			case Inst:
				fv := time.Time(v)
				field.Set(reflect.ValueOf(&fv))
			case ID:
				pointer, ok := a.pointers[v]
				if ok {
					field.Set(pointer)
				} else {
					pointer := a.allocate(v, field.Type())
					field.Set(pointer)
				}
			default:
				err = NewError("assembler.invalidFactPointerValue")
				return
			}
		} else {
			switch v := datum.V.(type) {
			case String:
				field.SetString(string(v))
			case Int:
				// TODO what about overflows?
				field.SetInt(int64(v))
			case Bool:
				field.SetBool(bool(v))
			case Float:
				field.SetFloat(float64(v))
			case Inst:
				field.Set(reflect.ValueOf(time.Time(v)))
			case ID:
				switch {
				case attr.Ident == sys.DbId:
					field.SetUint(uint64(v))
				case attr.IsMap():
					var m reflect.Value
					if field.IsNil() {
						// TODO ask the snapshot how many EA exist to size the map exactly.
						m = reflect.MakeMap(field.Type())
						field.Set(m)
					} else {
						m = field
					}
					mapValueType := m.Type().Elem()
					mapHasPointers := mapValueType.Kind() == reflect.Pointer
					if mapHasPointers {
						mapValueType = mapValueType.Elem()
					}
					pointer, ok := a.pointers[v]
					if !ok {
						pointer = a.allocate(v, reflect.PointerTo(mapValueType))
					}
					a.addEntityToMap(attr.MapKey, m, v, pointer, mapHasPointers, ok)
				case attr.IsSlice():
					var slice reflect.Value
					if field.IsNil() {
						// TODO ask the snapshot how many EA exist to size the slice exactly.
						slice = reflect.MakeSlice(field.Type(), 0, 1)
						field.Set(slice)
					} else {
						slice = field
					}
					if attr.CollValue == "" {
						sliceValueType := slice.Type().Elem()
						pointer, ok := a.pointers[v]
						if !ok {
							pointer = a.allocate(v, reflect.PointerTo(sliceValueType))
						}
						a.addEntityToSlice(attr.CollValue, slice, v, pointer, ok)
					} else {
						// Since we have exactly two facts to find, we can reasonably just go right to them,
						// though we may want to mark the entity id as processed now.
						scalar := a.findValue(v, attr.CollValue)
						i := int(a.findValue(v, Ident("sys/db/rank")).(int64))
						slice.Index(i).Set(reflect.ValueOf(scalar))
					}
				default:
					pointer, ok := a.pointers[v]
					if ok {
						field.Set(pointer.Elem())
					} else {
						pointer = field.Addr()
						a.pointers[v] = pointer
						a.unprocessed[v] = pointer
					}
				}
			default:
				err = NewError("assembler.invalidFactValue")
				return
			}
		}
	}
	maes, ok := a.mapsAwaitingEntries[id]
	if ok {
		for _, mae := range maes {
			a.addEntityToMap(mae.mapKey, mae.m, id, mae.pointer, mae.mapHasPointers, true)
		}
		delete(a.mapsAwaitingEntries, id)
	}
	saes, ok := a.slicesAwaitingEntries[id]
	if ok {
		for _, sae := range saes {
			a.addEntityToSlice(sae.collValue, sae.slice, id, sae.pointer, true)
		}
		delete(a.slicesAwaitingEntries, id)
	}
	a.instances[id] = ptr
	return
}

func (as *assembler) findValue(e ID, a Ident) (v any) {
	// TODO snapshot should support SelectOne?
	iter := as.snapshot.Select(Claim{E: e, A: a})
	if !iter.Next() {
		return
	}
	switch x := iter.Value().V.(type) {
	case String:
		v = string(x)
	case Int:
		v = int64(x)
	case Bool:
		v = bool(x)
	case Float:
		v = float64(x)
	case Inst:
		v = time.Time(x)
	case ID:
		v = uint64(x)
	default:
		panic("assembler.invalidFactValue")
	}
	return
}

func (a *assembler) addEntityToMap(mapKey Ident, m reflect.Value, id ID, pointer reflect.Value, mapHasPointers bool, immediate bool) {
	if immediate {
		// findValue is the only way of finding the key value when it's not present on the value struct,
		// though is plausibly much less efficient when that is the case. It might be useful to optimize
		// that common case by constructing a more robust (cached) model of a struct's attributes that
		// allows lookup by ident and use that to lookup the field value by index here.
		key := a.findValue(id, mapKey)
		value := pointer
		if !mapHasPointers {
			value = pointer.Elem()
		}
		m.SetMapIndex(reflect.ValueOf(key), value)
		return
	}
	mae := mapAwaitingEntry{mapKey, m, pointer, mapHasPointers}
	maes, ok := a.mapsAwaitingEntries[id]
	if !ok {
		a.mapsAwaitingEntries[id] = []mapAwaitingEntry{mae}
	} else {
		maes = append(maes, mae)
		a.mapsAwaitingEntries[id] = maes
	}
}

func (a *assembler) addEntityToSlice(collValue Ident, slice reflect.Value, id ID, pointer reflect.Value, immediate bool) {
	if immediate {
		value := pointer.Elem()
		index := a.findValue(id, Ident("sys/db/rank"))
		i := int(index.(int64))
		slice.Index(i).Set(value)
		return
	}
	sae := sliceAwaitingEntry{collValue, slice, pointer}
	saes, ok := a.slicesAwaitingEntries[id]
	if !ok {
		a.slicesAwaitingEntries[id] = []sliceAwaitingEntry{sae}
	} else {
		saes = append(saes, sae)
		a.slicesAwaitingEntries[id] = saes
	}
}

func Assemble[T any](a *assembler, id ID, entityPointer *T) (entity T, err error) {
	pointerType := reflect.TypeOf(entityPointer)
	if pointerType.Kind() != reflect.Pointer {
		err = NewError("assembler.destNotPointer")
		return
	}
	structType := pointerType.Elem()
	_, modelErr := a.analyzer.Analyze(structType)
	if modelErr != nil {
		err = modelErr
		return
	}
	instance, ok := a.instances[id]
	if !ok {
		a.allocate(id, pointerType)
		err = a.assembleAll()
		if err != nil {
			return
		}
	}
	extant := instance.Elem().Interface()
	entity, ok = extant.(T)
	if !ok {
		err = NewError("assembler.typeConflictForID", "id", id, "extant", extant, "type", structType)
	}
	return
}
