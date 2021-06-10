package arrow_marshal

import (
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/shopspring/decimal"
	"reflect"
	"time"
)

func UnmarshalRecords(record array.Record, v interface{}) ([]interface{}, error) {
	rv := reflect.TypeOf(v)

	arr := make([]interface{}, 0, record.NumRows())

	columnNameMap := make(map[string]int)

	for columnIndex := range record.Columns() {
		columnNameMap[record.ColumnName(columnIndex)] = columnIndex
	}
	for rowIndex := int64(0); rowIndex < record.NumRows(); rowIndex++ {
		val := reflect.New(rv)
		for fieldIndex := 0; fieldIndex < rv.NumField(); fieldIndex++ {
			f := rv.Field(fieldIndex)
			fieldName := f.Tag.Get("arrow")
			if len(fieldName) == 0 {
				continue
			}
			if nameIndex, ok := columnNameMap[fieldName]; ok {
				data := record.Column(nameIndex).Data()
				if data == nil {
					continue
				}
				v, _ := getDataValue(record.Column(nameIndex).Data(), int(rowIndex))
				field := val.Elem().Field(fieldIndex)
				va := reflect.ValueOf(v)
				if field.CanSet() {
					field.Set(va)
				}
			}
		}
		arr = append(arr, val.Interface())
	}

	return arr, nil
}

func getDataValue(data *array.Data, index int) (interface{}, error) {
	switch data.DataType().ID() {
	case arrow.DATE32:
		d := array.NewDate32Data(data)
		if d.IsNull(index) {
			return nil, fmt.Errorf("failed to convert to date32")
		}
		return d.Value(index), nil
	case arrow.STRING:
		d := array.NewStringData(data)
		return d.Value(index), nil
	case arrow.TIMESTAMP:
		dt := data.DataType().(*arrow.TimestampType)
		timezone, _ := time.LoadLocation(dt.TimeZone)
		if timezone == nil {
			timezone = time.UTC
		}
		d := array.NewTimestampData(data)
		value := d.Value(index)
		t := time.Unix(0, int64(value) * int64(time.Millisecond)).In(timezone)
		return t, nil
	case arrow.INT32:
		d := array.NewInt32Data(data)
		return d.Value(index), nil
	case arrow.INT64:
		d := array.NewInt64Data(data)
		return d.Value(index), nil
	case arrow.TIME64:
		d := array.NewTime64Data(data)
		return d.Value(index), nil
	case arrow.TIME32:
		d := array.NewTime32Data(data)
		return d.Value(index), nil
	case arrow.DECIMAL:
		dt := data.DataType().(*arrow.Decimal128Type)
		if dt == nil {
			return nil, fmt.Errorf("failed to convert decimal")
		}
		d := array.NewDecimal128Data(data)
		deci := decimal.New(int64(d.Value(index).LowBits()), -dt.Scale)
		return deci, nil
	default:
		return nil, fmt.Errorf("unkown arrow type %s", data.DataType().ID().String())
	}
}
