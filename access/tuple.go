package access

import (
	"LearnPG/errf"
	"fmt"
)

const (
	uz uint32 = 0
)

type DatumTupleFields struct {
}

type HeapTupleFields struct {
	xMin TxID
	xMax TxID
	ID   interface{} // Command ID or tx Vaccum ID
}

type HeapTupleHeader struct {
	TupleFields interface{}
	CurrentTID  ItemPointerData
	InfoMask2   uint16
	InfoMask    uint16
	NULLBits    []uint8
	Padding     []byte
}

type HeapTuple struct {
	DataLen  uint32
	Self     ItemPointerData
	TableOID OID
	Data     HeapTupleHeader
}

func HeapFormTuple(desc *TupleDesc, values []*Datum, isNull []bool) *HeapTuple {
	var tuple *HeapTuple = &HeapTuple{}
	tuple.Data = HeapTupleHeader{}

	numOfAttr := desc.NAttr

	if numOfAttr > MaxTupleAttributeNumber {
		panic(fmt.Sprintf("number of colums (%d) exceeds limit (%d)", numOfAttr, MaxTupleAttributeNumber))
	}

	if isNull != nil {
		var hasNull = false
		for i := uz; i < numOfAttr; i++ {
			if isNull[i] {
				hasNull = true
				break
			}
		}

		if hasNull {
			tuple.Data.NULLBits = make([]byte, AlignByteLen(uintptr(numOfAttr)))
		}
	}
	tuple.DataLen = uint32(HeapComputeDataSize(desc, values, isNull))
	tuple.Data.Padding = make([]byte, tuple.DataLen)
	tuple.Self = nil
	tuple.TableOID = InvalidOID

	td := &tuple.Data
	td.CurrentTID = nil
	// Set Datum length, TypeID, Mod, and NAttributes
	// Hoff is not needed in Golang, since we do not need offset for Data field.

	return tuple.HeapFillTuple(desc, values, isNull)
}

func (c *HeapTuple) HeapFillTuple(desc *TupleDesc, values []*Datum, isNull []bool) *HeapTuple {
	if c.Data.NULLBits != nil {
		for i := uz; i < c.DataLen; i++ {
			c.Data.SetNull(i, isNull[i])
		}
	}
	c.Data.InfoMask = 0
	c.Data.InfoMask2 = 0
	offset := uz
	for i := uz; i < desc.NAttr; i++ {
		attrLen := desc.GetAttr(i).Len
		data := values[i].buf.Bytes()
		c.Data.Padding = append(c.Data.Padding, data...)
		offset += attrLen
	}
	errf.Assert(offset == c.DataLen,
		fmt.Sprintf("heap filling got errf: the total offset (%d) does not match value length (%d)", offset, c.DataLen))
	return c
}

func HeapComputeDataSize(desc *TupleDesc, values []*Datum, isNull []bool) uintptr {
	var dataLen uintptr = 0
	for i := uz; i < desc.NAttr; i++ {
		if isNull != nil && isNull[i] {
			continue
		}
		attr := desc.GetAttr(i)
		dataLen += uintptr(attr.Len)
	}
	return dataLen
}
