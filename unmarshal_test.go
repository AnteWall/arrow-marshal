package arrow_marshal

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestUnmarshalRecords_Int32(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)
	col1 := func() array.Interface {
		ib := array.NewInt32Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewInt32Array()
	}()
	defer col1.Release()

	cols := []array.Interface{col1}
	rec := array.NewRecord(schema, cols, 10)
	defer rec.Release()

	type MyData struct {
		IntData int32 `arrow:"f1-i32"`
	}

	data, err := UnmarshalRecords(rec, MyData{})

	assert.NoError(t, err)
	assert.Equal(t, 10, len(data))
	assert.Equal(t, int32(1), data[0].(*MyData).IntData)
	assert.Equal(t, int32(2), data[1].(*MyData).IntData)
}

func TestUnmarshalRecords_Int64(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "f1-i64", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
	col1 := func() array.Interface {
		ib := array.NewInt64Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewInt64Array()
	}()
	defer col1.Release()

	cols := []array.Interface{col1}
	rec := array.NewRecord(schema, cols, 10)
	defer rec.Release()

	type MyData struct {
		IntData int64 `arrow:"f1-i64"`
	}

	data, err := UnmarshalRecords(rec, MyData{})

	assert.NoError(t, err)
	assert.Equal(t, 10, len(data))
	assert.Equal(t, int64(1), data[0].(*MyData).IntData)
	assert.Equal(t, int64(2), data[1].(*MyData).IntData)
}

func TestUnmarshalRecords_String(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "f1-str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	col1 := func() array.Interface {
		ib := array.NewStringBuilder(mem)
		defer ib.Release()

		ib.AppendValues([]string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, nil)
		return ib.NewStringArray()
	}()
	defer col1.Release()

	cols := []array.Interface{col1}
	rec := array.NewRecord(schema, cols, 10)
	defer rec.Release()

	type MyData struct {
		StringData string `arrow:"f1-str"`
	}

	data, err := UnmarshalRecords(rec, MyData{})

	assert.NoError(t, err)
	assert.Equal(t, 10, len(data))
	assert.Equal(t, "1", data[0].(*MyData).StringData)
	assert.Equal(t, "2", data[1].(*MyData).StringData)
}

func TestUnmarshalRecords_TimestampMS(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "f1-ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
		},
		nil,
	)
	col1 := func() array.Interface {
		timestampType := arrow.TimestampType{
			Unit:     arrow.Millisecond,
			TimeZone: "UTC",
		}
		ib := array.NewTimestampBuilder(mem, &timestampType)
		defer ib.Release()
		vs := make([]arrow.Timestamp, 1)
		vs[0] = arrow.Timestamp(1623312219000)
		ib.AppendValues(vs, nil)
		return ib.NewTimestampArray()
	}()
	defer col1.Release()

	cols := []array.Interface{col1}
	rec := array.NewRecord(schema, cols, 1)
	defer rec.Release()

	type MyData struct {
		Timestamp time.Time `arrow:"f1-ts"`
	}

	data, err := UnmarshalRecords(rec, MyData{})
	expectedTime := time.Unix(1623312219, 0)
	utc, _ := time.LoadLocation("UTC")
	expectedTime = expectedTime.In(utc)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data))
	assert.Equal(t, expectedTime, data[0].(*MyData).Timestamp)
}
