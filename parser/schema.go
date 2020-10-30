package parser

type DataType struct {
	Dtype string
	Size  int
}

// Schema represents a parsed schema query
type Schema struct {
	Use       bool
	TableOrDB string
	Name      string
	Columns   map[string]DataType
}
