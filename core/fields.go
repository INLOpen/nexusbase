package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"
)

type PointTypeValue byte

const (
	PointTypeValueNil    PointTypeValue = 0x00
	PointTypeValueFloat  PointTypeValue = 0x01
	PointTypeValueInt    PointTypeValue = 0x02
	PointTypeValueString PointTypeValue = 0x03
	PointTypeValueBool   PointTypeValue = 0x04
)

type FieldValues map[string]PointValue

var (
	// pool of FieldValues maps to reduce map allocations under heavy write load.
	fieldValuesPool = sync.Pool{
		New: func() interface{} { return make(FieldValues) },
	}
	// buffer pool for Encode to avoid allocating bytes.Buffers frequently.
	fieldEncodeBufPool = sync.Pool{
		New: func() interface{} { return new(bytes.Buffer) },
	}
)

// GetPooledFieldValues returns a FieldValues map from the internal pool.
// Caller should call PutPooledFieldValues when finished to enable reuse.
func GetPooledFieldValues() FieldValues {
	v := fieldValuesPool.Get()
	if v == nil {
		return make(FieldValues)
	}
	return v.(FieldValues)
}

// PutPooledFieldValues resets and returns a FieldValues map to the pool.
func PutPooledFieldValues(fv FieldValues) {
	if fv == nil {
		return
	}
	for k := range fv {
		delete(fv, k)
	}
	fieldValuesPool.Put(fv)
}

// MarshalJSON implements the json.Marshaler interface for PointValue.
func (pv PointValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(pv.data)
}

// Encode serializes the FieldValues map into a byte slice.
func (fv FieldValues) Encode() ([]byte, error) {
	// Reuse a bytes.Buffer from the pool to avoid frequent allocations.
	bp := fieldEncodeBufPool.Get().(*bytes.Buffer)
	buf := bp
	buf.Reset()
	defer fieldEncodeBufPool.Put(bp)

	if err := binary.Write(buf, binary.BigEndian, uint16(len(fv))); err != nil {
		return nil, fmt.Errorf("failed to write field count: %w", err)
	}

	for k, v := range fv {
		keyBytes := []byte(k)
		if err := binary.Write(buf, binary.BigEndian, uint16(len(keyBytes))); err != nil {
			return nil, fmt.Errorf("failed to write key length for '%s': %w", k, err)
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return nil, fmt.Errorf("failed to write key bytes for '%s': %w", k, err)
		}
		valueByte, err := encodeValue(v.valueType, v.data)
		if err != nil {
			return nil, fmt.Errorf("failed to encode value for key '%s': %w", k, err)
		}

		buf.WriteByte(byte(v.valueType))
		if _, err := buf.Write(valueByte); err != nil {
			return nil, fmt.Errorf("failed to write value bytes for '%s': %w", k, err)
		}
	}

	// Copy out the buffer contents to a fresh slice since callers may retain
	// the returned slice independently of the pool. Returning buf.Bytes()
	// directly would expose pooled backing memory.
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

func (fv FieldValues) FromMap(data map[string]interface{}) error {
	for k, v := range data {
		pv, err := NewPointValue(v)
		if err != nil {
			return fmt.Errorf("invalid value for field '%s': %w", k, err)
		}
		fv[k] = pv
	}
	return nil
}

// DecodeFields deserializes a byte stream from a reader into a FieldValues map.
func DecodeFields(r io.Reader) (FieldValues, error) {
	var numPairs uint16
	if err := binary.Read(r, binary.BigEndian, &numPairs); err != nil {
		return nil, fmt.Errorf("failed to read field count: %w", err)
	}

	fields := make(FieldValues, numPairs)

	for i := 0; i < int(numPairs); i++ {
		// Read key
		var keyLen uint16
		if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read key length for pair %d: %w", i, err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key bytes for pair %d: %w", i, err)
		}
		key := string(keyBytes)

		// Read value type
		var valueTypeByte byte
		if err := binary.Read(r, binary.BigEndian, &valueTypeByte); err != nil {
			return nil, fmt.Errorf("failed to read value type for key '%s': %w", key, err)
		}
		valueType := PointTypeValue(valueTypeByte)

		// Read value data based on type
		val, err := decodeValue(valueType, r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value for key '%s': %w", key, err)
		}

		pv, err := NewPointValue(val)
		if err != nil {
			return nil, fmt.Errorf("failed to create point value for key '%s': %w", key, err)
		}
		fields[key] = pv
	}

	return fields, nil
}

// DecodeFieldsFromBytes is a helper function to decode from a byte slice.
func DecodeFieldsFromBytes(data []byte) (FieldValues, error) {
	buf := bytes.NewBuffer(data)
	return DecodeFields(buf)
}

// PointValue holds a typed value. The data field holds the actual Go type (e.g., float64, int64, string).
type PointValue struct {
	valueType PointTypeValue
	data      any
}

// NewPointValue creates a new PointValue by encoding the provided data.
func NewPointValue(data any) (PointValue, error) {
	var pv PointValue

	switch v := data.(type) {
	case float64:
		pv.valueType = PointTypeValueFloat
		pv.data = v
	case float32:
		pv.valueType = PointTypeValueFloat
		pv.data = float64(v) // Promote to float64
	case int:
		pv.valueType = PointTypeValueInt
		pv.data = int64(v) // Promote to int64
	case int64:
		pv.valueType = PointTypeValueInt
		pv.data = v
	case string:
		pv.valueType = PointTypeValueString
		pv.data = v
	case bool:
		pv.valueType = PointTypeValueBool
		pv.data = v
	case nil:
		return PointValue{valueType: PointTypeValueNil}, nil
	default:
		return PointValue{}, &UnsupportedTypeError{Message: fmt.Sprintf("unsupported value type: %T", data)}
	}
	return pv, nil
}

// encodeValue serializes a typed value into its byte representation.
func encodeValue(t PointTypeValue, data any) ([]byte, error) {
	switch t {
	case PointTypeValueFloat:
		v := data.(float64)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(v))
		return buf, nil
	case PointTypeValueInt:
		v := data.(int64)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf, nil
	case PointTypeValueString:
		vStr := data.(string)
		// Pre-allocate buffer: 4 bytes for length + length of string
		buf := make([]byte, 4+len(vStr))
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(vStr)))
		copy(buf[4:], vStr)
		return buf, nil
	case PointTypeValueBool:
		v := data.(bool)
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	default: // This handles PointTypeValueNil
		return nil, nil
	}
}

// decodeValue deserializes a byte slice back into its Go type.
func decodeValue(t PointTypeValue, r io.Reader) (any, error) {
	switch t {
	case PointTypeValueFloat:
		var f float64
		if err := binary.Read(r, binary.BigEndian, &f); err != nil {
			return nil, err
		}
		return f, nil
	case PointTypeValueInt:
		var i int64
		if err := binary.Read(r, binary.BigEndian, &i); err != nil {
			return nil, err
		}
		return i, nil
	case PointTypeValueString:
		var length uint32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return nil, fmt.Errorf("failed to read string length: %w", err)
		}
		strBytes := make([]byte, length)
		if _, err := io.ReadFull(r, strBytes); err != nil {
			return nil, fmt.Errorf("failed to read string data: %w", err)
		}
		return string(strBytes), nil
	case PointTypeValueBool:
		var b byte
		if err := binary.Read(r, binary.BigEndian, &b); err != nil {
			return nil, err
		}
		return b == 1, nil
	case PointTypeValueNil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported value type for decoding: %v", t)
	}
}

// ValueString returns the value as a string, if it is of that type.
func (pv PointValue) ValueString() (string, bool) {
	val, ok := pv.data.(string)
	return val, ok
}

func (pv PointValue) ValueFloat64() (float64, bool) {
	val, ok := pv.data.(float64)
	return val, ok
}

func (pv PointValue) ValueInt64() (int64, bool) {
	val, ok := pv.data.(int64)
	return val, ok
}

func (pv PointValue) ValueBool() (bool, bool) {
	val, ok := pv.data.(bool)
	return val, ok
}

func (pv PointValue) IsNull() bool {
	return pv.valueType == PointTypeValueNil
}

func (pv PointValue) Bytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, pv.valueType); err != nil {
		return nil, err
	}
	switch pv.valueType {
	case PointTypeValueFloat:
		if err := binary.Write(buf, binary.BigEndian, pv.data.(float64)); err != nil {
			return nil, err
		}
	case PointTypeValueInt:
		if err := binary.Write(buf, binary.BigEndian, pv.data.(int64)); err != nil {
			return nil, err
		}
	case PointTypeValueString:
		// write length of string
		str := pv.data.(string)
		if err := binary.Write(buf, binary.BigEndian, uint32(len(str))); err != nil {
			return nil, err
		}
		// write string
		if _, err := buf.WriteString(str); err != nil {
			return nil, err
		}
	case PointTypeValueBool:
		if err := binary.Write(buf, binary.BigEndian, pv.data.(bool)); err != nil {
			return nil, err
		}
	case PointTypeValueNil:
		// nothing to write
	default:
	}

	return buf.Bytes(), nil
}

// ToMap converts FieldValues to a standard map[string]interface{}.
func (fv FieldValues) ToMap() map[string]interface{} {
	m := make(map[string]interface{}, len(fv))
	for k, v := range fv {
		m[k] = v.data
	}
	return m
}

// NewFieldValuesFromMap is a helper to create FieldValues from a standard map.
func NewFieldValuesFromMap(data map[string]interface{}) (FieldValues, error) {
	if data == nil {
		return nil, nil
	}
	fv := GetPooledFieldValues()
	// ensure capacity by creating a new map when incoming size is larger
	if len(fv) < len(data) {
		fv = make(FieldValues, len(data))
	}
	for k, v := range data {
		pv, err := NewPointValue(v)
		if err != nil {
			// return pooled map to avoid leak
			PutPooledFieldValues(fv)
			return nil, fmt.Errorf("invalid value for field '%s': %w", k, err)
		}
		fv[k] = pv
	}
	return fv, nil
}
