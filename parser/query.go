package parser

// Query is of the form
// Select/Update/insert/delete ____, ____
// from _____
// where condition 1 , conditon 2 , ______
// Query represents a parsed query
type Query struct {
	Type                      Type
	TableName                 string
	Conditions                []Condition
	Updates                   map[string]string
	Inserts                   [][]string
	Fields                    []string // Used for SELECT (i.e. SELECTed field names) and INSERT (INSERTEDed field names)
	ConditionOperators        []string //AND or OR between two conditions // e.g if WHERE condition1 AND condition2 OR condition3 , then this array is ["AND","OR"]
	AggregateFunc             map[string][]string
	GroupByField              []string
	HavingConditions          []HavingCondition
	HavingConditionsOperators []string //AND or OR
}

// Type is the type of SQL query, e.g. SELECT/UPDATE
type Type int

const (
	// UnknownType is the zero value for a Type
	UnknownType Type = iota
	// Select represents a SELECT query
	Select
	// Update represents an UPDATE query
	Update
	// Insert represents an INSERT query
	Insert
	// Delete represents a DELETE query
	Delete
	// Drop Table
	Drop
)

// TypeString is a string slice with the names of all types in order
var TypeString = []string{
	"UnknownType",
	"Select",
	"Update",
	"Insert",
	"Delete",
	"Drop",
}

// Operator is between operands in a condition
type Operator int

const (
	// UnknownOperator is the zero value for an Operator
	UnknownOperator Operator = iota
	// Eq -> "="
	Eq
	// Ne -> "!="
	Ne
	// Gt -> ">"
	Gt
	// Lt -> "<"
	Lt
	// Gte -> ">="
	Gte
	// Lte -> "<="
	Lte
)

// OperatorString is a string slice with the names of all operators in order
var OperatorString = []string{
	"UnknownOperator",
	"Eq",
	"Ne",
	"Gt",
	"Lt",
	"Gte",
	"Lte",
}

// Condition is a single boolean condition in a WHERE clause
type Condition struct {
	// Operand1 is the left hand side operand
	Operand1 string
	// Operand1IsField determines if Operand1 is a literal or a field name
	Operand1IsField bool
	// Operator is e.g. "=", ">"
	Operator Operator
	// Operand1 is the right hand side operand
	Operand2 string
	// Operand2IsField determines if Operand2 is a literal or a field name
	Operand2IsField bool
}

// HavingCondition is a single boolean condition in a Having clause
type HavingCondition struct {
	// OperandField1 is the left hand side operand
	OperandField1 string
	// OperandAggFunc defines the aggregate function on OperandField1
	OperandAggFunc string
	// Operand1IsField determines if Operand1 is a literal or a field name
	Operator Operator
	// Operand2 is the right hand side operand . Here it is defined as string but it will be a interger or floating
	Operand2 string
}
