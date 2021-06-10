# Go Arrow Unmarshal

[![Actions Status](https://github.com/antewall/arrow-marshal/workflows/test/badge.svg)](https://github.com/antewall/arrow-marshal/actions)
[![codecov](https://codecov.io/gh/antewall/arrow-marshal/branch/master/graph/badge.svg)](https://codecov.io/gh/antewall/arrow-marshal)

Go package to unmarshal arrow records into array of structs. Can use tags to specify fields

### Example

```go
type MyData struct {
    MyInt       int32       `arrow:"int_field"`
    MyFloat     float32     `arrow:"float_field"`    
    Timestamp   time.Time   `arrow:"ts_field"`
}

data, err := arrow_marshal.UnmarshalRecords(records, MyData{})
```
