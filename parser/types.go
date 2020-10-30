package parser

type parser struct {
	i               int
	sql             string
	step            step
	query           Query
	err             error
	nextUpdateField string
	nextAggFunc     string
}

type schemaParser struct {
	i               int
	sql             string
	schema          Schema
	err             error
	step            schemaStep
	currentField    string
	currentDataType DataType
}

type step int

const (
	stepType step = iota
	stepSelectField
	stepSelectFrom
	stepSelectComma
	stepSelectFromTable
	stepInsertTable
	stepInsertFieldsOpeningParens
	stepInsertFields
	stepInsertFieldsCommaOrClosingParens
	stepInsertValuesOpeningParens
	stepInsertValuesRWord
	stepInsertValues
	stepInsertValuesCommaOrClosingParens
	stepInsertValuesCommaBeforeOpeningParens
	stepUpdateTable
	stepUpdateSet
	stepUpdateField
	stepUpdateEquals
	stepUpdateValue
	stepUpdateComma
	stepDeleteFromTable
	stepWhere
	stepWhereField
	stepWhereOperator
	stepWhereValue
	stepWhereAnd
	stepWhereOr
	stepAggregateFunc
	stepSelectAggrOpenParens
	stepSelectAggField
	stepSelectAggrClosingParens
	stepSelectGroupByField
	stepSelectGroupBy
	stepSelectGroupByComma
	stepGroupByHaving
	stepHavingAggregateFunc
	stepHavingAggrOpenParens
	stepHavingAggField
	stepHavingAggrClosingParens
	stepHavingOperator
	stepHavingValue
	stepHavingAnd
	stepHavingOr
)

var reservedWords = []string{
	"(", ")", ">=", "<=", "!=", ",", "=", ">", "<", "SELECT", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM",
	"WHERE", "FROM", "SET", "AND", "OR", "GROUP BY", "HAVING",
}

var aggFunc = []string{
	"COUNT", "AVG", "SUM",
}

type schemaStep int

const (
	stepCreate schemaStep = iota
	stepCreateTable
	stepTableName
	stepCreateOpenParens
	stepTableColumn
	stepColumnType
	stepColumnTypeOpenParens
	stepColumnTypeSize
	stepColumnTypeCloseParens
	stepColumnComma
	stepCreateCloseParens
	stepEnd
)

var dataTypes = []string{
	"int", "varchar", "char", "boolean",
}

var schemaReserveWords = []string{
	"CREATE", "TABLE", "create", "table", "(", ")", ",",
}
