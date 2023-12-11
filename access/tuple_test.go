package access

import (
	"LearnPG/errf"
	"math/rand"
	"testing"
)

func NewTestTupleDescValuePair(n uint32) (*TupleDesc, []*Datum) {
	var err error
	resDesc := &TupleDesc{
		NAttr:        n,
		TypeID:       InvalidOID,
		TypeModifier: InvalidOID,
		TupleCons:    nil,
		RefCount:     -1,
		Attrs:        make([]FormDataAttribute, n),
	}
	value := make([]*Datum, n)
	for i := uz; i < n; i++ {
		pw := rand.Intn(3)
		bits := 1 << pw
		resDesc.Attrs[i].Len = uint32(bits) // int8, int16, int32
		if pw == 0 {
			curVal := byte(rand.Int() % (1 << bits))
			value[i], err = PackDatum(curVal)
		} else if pw == 1 {
			curVal := uint16(rand.Int() % (1 << bits))
			value[i], err = PackDatum(curVal)
		} else if pw == 2 {
			curVal := uint32(rand.Int() % (1 << bits))
			value[i], err = PackDatum(curVal)
		}
		if err != nil {
			panic(err)
		}
	}
	return resDesc, value
}

func TestHeapFormTuple(t *testing.T) {
	for i := 0; i < 100; i++ {
		attrLen := uint32(5)
		desc, values := NewTestTupleDescValuePair(attrLen)
		tuple := HeapFormTuple(desc, values, nil)
		errf.Jprint(tuple)
	}
}
