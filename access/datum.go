package access

import (
	"bytes"
	"encoding/binary"
)

type OID uint

const (
	InvalidOID = 0
	ByteMask   = (1 << ByteLen) - 1
)

type Datum struct {
	buf *bytes.Buffer
}

func PackDatum(v any) (*Datum, error) {
	res := Datum{buf: new(bytes.Buffer)}
	err := binary.Write(res.buf, binary.BigEndian, v)
	return &res, err
}

func (c *Datum) UnPackDatum(ptr any) error {
	return binary.Read(c.buf, binary.BigEndian, ptr)
}
