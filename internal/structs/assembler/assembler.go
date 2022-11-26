// Package assembler provides for the construction of structs from sets of datums.
package assembler

import (
	"reflect"
	"sort"
	"time"

	"github.com/dball/destructive/internal/structs/attrs"
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

type indexedAttrTag struct {
	attrs.AttrTag
	i int
}

type mapAwaitingEntry struct {
	attrTag indexedAttrTag
	m       reflect.Value
	pointer reflect.Value
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
}

func NewAssembler[T any](base *T, facts []Fact) (a *assembler[T], err error) {
	ptr := reflect.ValueOf(base)
	if ptr.Kind() != reflect.Pointer {
		err = NewError("assembler.baseNotPointer")
		return
	}
	// TODO test that the pointer value type is a struct
	a = &assembler[T]{
		base:                base,
		facts:               facts,
		instances:           map[ID]*T{},
		pointers:            map[ID]reflect.Value{},
		unprocessed:         map[ID]reflect.Value{},
		mapsAwaitingEntries: map[ID][]mapAwaitingEntry{},
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

	// TODO the assembler should cache these
	typ := value.Type()
	n := typ.NumField()
	attrTags := make(map[Ident]indexedAttrTag, n)
	for i := 0; i < n; i++ {
		field := typ.Field(i)
		attrTag, attrErr := attrs.ParseAttrField(field)
		if attrErr != nil {
			err = attrErr
			return
		}
		if attrTag.Ident != "" {
			attrTags[attrTag.Ident] = indexedAttrTag{AttrTag: attrTag, i: i}
		}
	}

	offset := sort.Search(len(a.facts), func(i int) bool { return a.facts[i].E >= id })
	total := len(a.facts)
	for i := offset; i < total; i++ {
		fact := a.facts[i]
		if fact.E != id {
			break
		}
		if i == offset {
			attrTag, ok := attrTags[Ident(sys.DbId)]
			if ok {
				field := value.Field(attrTag.i)
				field.SetUint(uint64(id))
			}
		}
		attrTag, ok := attrTags[fact.A]
		if !ok {
			// We don't care if we get a fact with no field.
			continue
		}
		field := value.Field(attrTag.i)
		if attrTag.Pointer {
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
				case attrTag.Ident == sys.DbId:
					field.SetUint(uint64(v))
				case attrTag.MapKey != "":
					if field.Kind() != reflect.Map {
						err = NewError("assembler.invalidFactMapValue")
						return
					}
					var m reflect.Value
					if field.IsNil() {
						m = reflect.MakeMap(field.Type())
						field.Set(m)
					} else {
						// TODO is this right? it feels weird.
						m = field
					}
					pointer, ok := a.pointers[v]
					if !ok {
						// TODO pointerTo won't be right if the map contains pointers, not structs
						pointer = a.allocate(v, reflect.PointerTo(m.Type().Elem()))
					}
					a.addEntityToMap(attrTag, m, v, pointer, ok)
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
			a.addEntityToMap(mae.attrTag, mae.m, id, mae.pointer, true)
		}
		delete(a.mapsAwaitingEntries, id)
	}
	instance, ok := ptr.Interface().(*T)
	if ok {
		a.instances[id] = instance
	}
	return
}

func (a *assembler[T]) addEntityToMap(attrTag indexedAttrTag, m reflect.Value, id ID, pointer reflect.Value, immediate bool) {
	if immediate {
		value := pointer.Elem()
		// TODO the field index is just wrong, though it's coincidentally working wtf
		// but more to the point, the value retrieved from the map ultimately is an empty struct??
		key := value.Field(attrTag.i)
		m.SetMapIndex(key, value)
		return
	}
	mae := mapAwaitingEntry{attrTag, m, pointer}
	maes, ok := a.mapsAwaitingEntries[id]
	if !ok {
		a.mapsAwaitingEntries[id] = []mapAwaitingEntry{mae}
	} else {
		maes = append(maes, mae)
		a.mapsAwaitingEntries[id] = maes
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
