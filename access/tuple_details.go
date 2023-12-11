package access

const (
	MaxTupleAttributeNumber = 2000
	ByteLen                 = 8
)

func AlignLength(alignValue uintptr, len uintptr) uintptr {
	// alignValue shall be 2^x.
	//fmt.Printf("%b\n", len+alignValue-1)
	//fmt.Printf("%b\n", alignValue-1)
	//fmt.Printf("%b\n", (len+alignValue-1) & ^(alignValue-1))
	return (len + alignValue - 1) & ^(alignValue - 1)
}

func MaxAlign(len uintptr) uintptr {
	return AlignLength(ByteLen, len)
}

func AlignByteLen(len uintptr) uintptr {
	return MaxAlign(len) / ByteLen
}

func (c *HeapTupleHeader) SetNull(i uint32, isNull bool) {
	if c.NULLBits == nil {
		panic("the null bits are not initialized")
	}
	if isNull {
		c.NULLBits[i/ByteLen] |= 1 << (i % ByteLen)
	} else {
		c.NULLBits[i/ByteLen] &= (1<<ByteLen - 1) ^ (1 << (i % ByteLen))
	}
}
