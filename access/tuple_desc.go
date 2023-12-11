package access

// TODO:
// 1. implement the tuple constraints.
// 2. implement the tuple type and modifier.

// AttrNumber user defined attribute number, starting from 1.
type AttrNumber int16

type AttrDefault struct {
	Num AttrNumber
	Bin []byte
}

type Constraints struct {
}

type FormDataAttribute struct {
	Len uint32
}

// TupleDesc used to handle user attributes.
type TupleDesc struct {
	NAttr        uint32
	TypeID       OID // the TypeID indicates the specific type of fields
	TypeModifier int32
	TupleCons    *Constraints // constraints
	RefCount     int          // reference count, -1 for not counting.
	Attrs        []FormDataAttribute
}

func (c *TupleDesc) GetAttr(i uint32) *FormDataAttribute {
	return &c.Attrs[i]
}
