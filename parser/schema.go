package parser

type DataType struct {
	Dtype string
	Size  int
}

// Schema represents a parsed schema query
type Schema struct {
	TableName string
	Columns   map[string]DataType
}
