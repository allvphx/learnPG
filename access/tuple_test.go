package access

import (
	"LearnPG/err"
	"math/rand"
	"testing"
	"unsafe"
)

func NewTestTupleDescValuePair(n uint) (*TupleDesc, []Datum) {
	resDesc := &TupleDesc{
		NAttr:        n,
		TypeID:       InvalidOID,
		TypeModifier: InvalidOID,
		TupleCons:    nil,
		RefCount:     -1,
		Attrs:        make([]FormDataAttribute, n),
	}
	value := make([]Datum, n)
	for i := uz; i < n; i++ {
		pw := rand.Intn(3)
		bits := 1 << pw
		resDesc.Attrs[i].Len = uint(bits) // int8, int16, int32
		curVal := rand.Int() % (1 << bits)
		value[i] = Datum(unsafe.Pointer(&curVal))
	}
	return resDesc, value
}

func TestHeapFormTuple(t *testing.T) {
	for i := 0; i < 100; i++ {
		attrLen := uint(5)
		desc, values := NewTestTupleDescValuePair(attrLen)
		tuple := HeapFormTuple(desc, values, nil)
		err.Jprint(tuple)
	}
}
