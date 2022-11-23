package assembler

import (
	"reflect"
	"sort"
	"time"

	"github.com/dball/destructive/internal/structs/attrs"
	. "github.com/dball/destructive/internal/types"
)

type assembler2[T any] struct {
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
}

func NewAssembler2[T any](base *T, facts []Fact) (a *assembler2[T], err error) {
	ptr := reflect.ValueOf(base)
	if ptr.Kind() != reflect.Pointer {
		err = NewError("assembler.baseNotPointer")
		return
	}
	// TODO test that the pointer value type is a struct
	a = &assembler2[T]{
		base:        base,
		facts:       facts,
		instances:   map[ID]*T{},
		pointers:    map[ID]reflect.Value{},
		unprocessed: map[ID]reflect.Value{},
	}
	return a, err
}

func (a *assembler2[T]) allocate(id ID, pointerType reflect.Type) {
	// allocate the new pointer
	pp := reflect.New(pointerType)
	ptr := pp.Elem()
	// allocate the new struct
	entity := reflect.New(ptr.Type().Elem())
	// store the new struct in the new pointer
	ptr.Set(entity)
	// store the pointer in the unrealized entities map
	a.unprocessed[id] = ptr
	a.pointers[id] = ptr
}

func (a *assembler2[T]) assembleAll() {
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
		a.assemble(id, ptr)
	}
}

func (a *assembler2[T]) assemble(id ID, ptr reflect.Value) (err error) {
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
				pointer, ok := a.pointers[v]
				if ok {
					field.Set(pointer.Elem())
				} else {
					// TODO ugh there's probably some junk about struct field vs value pointers
					pointer = field.Addr()
					a.pointers[v] = pointer
					a.unprocessed[v] = pointer
				}
			default:
				err = NewError("assembler.invalidFactValue")
				return
			}
		}
	}
	instance, ok := ptr.Interface().(*T)
	if ok {
		a.instances[id] = instance
	}
	return
}

func (a *assembler2[T]) Next() (entity *T, err error) {
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
	a.assembleAll()
	instance, ok := a.instances[id]
	if !ok {
		err = NewError("assembler.noInstanceForId", "id", id)
		return
	}
	delete(a.instances, id)
	entity = instance
	return
}
