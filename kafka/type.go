package kafka

type TestStruct struct {
	Name string `json:"name"`
	Age  int64  `json:"age"`
}

type AvroStruct struct {
	Name string `avro:"field1"`
	Age  int64  `avro:"field2"`
}

var AvroSchema = `
{
	"type": "record",
	"name": "test_schema",
	"fields": [
		{
		"name": "field1",
		"type": "string"
		},
		{
		"name": "field2",
		"type": "int"
		}
	]
}`
