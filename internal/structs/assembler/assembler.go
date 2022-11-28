// Package assembler provides for the construction of structs from sets of datums.
package assembler

import (
	"reflect"
	"sort"
	"time"

	"github.com/dball/destructive/internal/structs/models"
	"github.com/dball/destructive/internal/sys"
	. "github.com/dball/destructive/internal/types"
)

// Fact is a datum whose attribute has been resolved to an ident and has lost
// its transaction.
//
// TODO This is probably not a good idea, probably better to use datums in the context
// of a database but we haven't defined database yet, so.
type Fact struct {
	E ID
	A Ident
	V Value
}

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

type assembler[T any] struct {
	// base is a (nil) pointer to a struct of the root entity type
	base *T
	// facts are the facts for the assembly, sorted by e
	facts []Fact
	// instances are the fully realized root entity instances not yet returned
	instances map[ID]*T
	// pointers are pointers to all of the at least partially realized entities allocated by the assembler
	pointers map[ID]reflect.Value
	// unprocessed are (not nil) pointers to unrealized entities
	unprocessed map[ID]reflect.Value
	// mapsAwaitingEntries are maps in entity struct fields awaiting referent entities to be realized
	mapsAwaitingEntries map[ID][]mapAwaitingEntry
	// slicesAwaitingEntries are slices in entity struct fields awaiting referent entities to be realized
	slicesAwaitingEntries map[ID][]sliceAwaitingEntry
}

func NewAssembler[T any](base *T, facts []Fact) (a *assembler[T], err error) {
	ptr := reflect.ValueOf(base)
	if ptr.Kind() != reflect.Pointer {
		err = NewError("assembler.baseNotPointer")
		return
	}
	// TODO test that the pointer value type is a struct
	a = &assembler[T]{
		base:                  base,
		facts:                 facts,
		instances:             map[ID]*T{},
		pointers:              map[ID]reflect.Value{},
		unprocessed:           map[ID]reflect.Value{},
		mapsAwaitingEntries:   map[ID][]mapAwaitingEntry{},
		slicesAwaitingEntries: map[ID][]sliceAwaitingEntry{},
	}
	return a, err
}

func (a *assembler[T]) allocate(id ID, pointerType reflect.Type) (ptr reflect.Value) {
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

func (a *assembler[T]) assembleAll() (err error) {
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

func (a *assembler[T]) assemble(id ID, ptr reflect.Value) (err error) {
	value := ptr.Elem()

	model, modelErr := models.Analyze(value.Type())
	if modelErr != nil {
		err = modelErr
		return
	}

	offset := sort.Search(len(a.facts), func(i int) bool { return a.facts[i].E >= id })
	total := len(a.facts)
	for i := offset; i < total; i++ {
		fact := a.facts[i]
		if fact.E != id {
			break
		}
		if i == offset {
			attr, ok := model.Attr(Ident(sys.DbId))
			if ok {
				field := value.Field(attr.Index)
				field.SetUint(uint64(id))
			}
		}
		attr, ok := model.Attr(fact.A)
		if !ok {
			// We don't care if we get a fact with no field.
			continue
		}
		field := value.Field(attr.Index)
		if attr.IsPointer() {
			// TODO who owns the vs anyway? If they're not copied at some point,
			// exposing value pointers opens the door to database corruption.
			switch v := fact.V.(type) {
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
			switch v := fact.V.(type) {
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
						n := 1
						for j := i + 1; j < total; j++ {
							f := a.facts[j]
							if f.E != fact.E || f.A != fact.A {
								break
							}
							n++
						}
						m = reflect.MakeMapWithSize(field.Type(), n)
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
						n := 1
						for j := i + 1; j < total; j++ {
							f := a.facts[j]
							if f.E != fact.E || f.A != fact.A {
								break
							}
							n++
						}
						slice = reflect.MakeSlice(field.Type(), n, n)
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
	instance, ok := ptr.Interface().(*T)
	if ok {
		a.instances[id] = instance
	}
	return
}

func (as *assembler[T]) findValue(e ID, a Ident) (v any) {
	i, ok := sort.Find(len(as.facts), func(i int) int {
		fact := as.facts[i]
		switch {
		case fact.E < e:
			return 1
		case fact.E > e:
			return -1
		}
		switch {
		case fact.A < a:
			return 1
		case fact.A > a:
			return -1
		}
		return 0
	})
	if ok {
		fact := as.facts[i]
		switch x := fact.V.(type) {
		// TODO cases
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
	}
	return
}

func (a *assembler[T]) addEntityToMap(mapKey Ident, m reflect.Value, id ID, pointer reflect.Value, mapHasPointers bool, immediate bool) {
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

func (a *assembler[T]) addEntityToSlice(collValue Ident, slice reflect.Value, id ID, pointer reflect.Value, immediate bool) {
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

func (a *assembler[T]) Next() (entity *T, err error) {
	if len(a.instances) != 0 {
		// TODO we should return these in id order, so we need a sorted map
		for id, instance := range a.instances {
			delete(a.instances, id)
			entity = instance
			return
		}
	}
	if len(a.facts) == 0 {
		return
	}
	// Find the first id that's not been assembled. Dubious assumption that
	// it must be of the root type though.
	// TODO figure out a good way to express the attrs for our root type.
	var id ID
	found := false
	for _, fact := range a.facts {
		if fact.E == id {
			continue
		}
		id = fact.E
		_, ok := a.pointers[id]
		if !ok {
			found = true
			break
		}
	}
	if !found {
		return
	}
	a.allocate(id, reflect.TypeOf(a.base))
	err = a.assembleAll()
	if err != nil {
		return
	}
	instance, ok := a.instances[id]
	if !ok {
		err = NewError("assembler.noInstanceForId", "id", id)
		return
	}
	delete(a.instances, id)
	entity = instance
	return
}
