package parser

import (
	"fmt"
	"regexp"
	"strings"
)

// Parse parses the given qury
func Parse(sqlq string) (Query, error) {
	parserobj := &parser{
		i:               0,
		sql:             sqlq,
		step:            stepType,
		query:           Query{},
		err:             nil,
		nextUpdateField: "",
	}

	return parserobj.parse()
}

func (p *parser) parse() (Query, error) {
	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	p.logError()
	return q, p.err
}

func (p *parser) doParse() (Query, error) {
	for {
		if p.i >= len(p.sql) {
			return p.query, p.err
		}

		switch p.step {
		case stepType:
			token, leng := p.getToken()
			switch strings.ToUpper(token) {
			case "SELECT":
				p.query.Type = Select
				p.pop(leng)
				p.step = stepAggregateFunc
				p.query.AggregateFunc = map[string]string{}
			case "INSERT INTO":
				p.query.Type = Insert
				p.pop(leng)
				p.step = stepInsertTable
			case "UPDATE":
				p.query.Type = Update
				p.query.Updates = map[string]string{}
				p.pop(leng)
				p.step = stepUpdateTable
			case "DELETE FROM":
				p.query.Type = Delete
				p.pop(leng)
				p.step = stepDeleteFromTable
			default:
				return p.query, fmt.Errorf("invalid query type")

			}
		case stepAggregateFunc:
			token, leng := p.getAggToken()
			if strings.ToUpper(token) == "COUNT" || strings.ToUpper(token) == "AVG" || strings.ToUpper(token) == "SUM" {
				p.step = stepSelectAggrOpenParens
				p.pop(leng)
				p.nextAggFunc = strings.ToUpper(token)
			} else {
				p.step = stepSelectField
			}
		case stepSelectAggrOpenParens:
			token, leng := p.getToken()
			if len(token) != 1 || token != "(" {
				return p.query, fmt.Errorf("at SELECT : expected opening parens")
			}
			p.pop(leng)
			p.step = stepSelectAggField
		case stepSelectAggField:
			identifier, leng := p.getToken()
			if !isIdentifierOrAsterisk(identifier) {
				return p.query, fmt.Errorf("at SELECT: expected field to SELECT")
			}
			p.query.Fields = append(p.query.Fields, identifier)
			p.query.AggregateFunc[p.nextAggFunc] = identifier
			p.pop(leng)
			p.step = stepSelectAggrClosingParens
		case stepSelectAggrClosingParens:
			token, leng := p.getToken()
			if len(token) != 1 || token != ")" {
				return p.query, fmt.Errorf("at SELECT : expected closing parens")
			}
			p.pop(leng)
			maybeFrom, leng := p.getToken()
			if strings.ToUpper(maybeFrom) == "FROM" {
				p.step = stepSelectFrom
				continue
			}
			p.step = stepSelectComma
		case stepSelectField:
			identifier, leng := p.getToken()
			if !isIdentifierOrAsterisk(identifier) {
				return p.query, fmt.Errorf("at SELECT: expected field to SELECT")
			}
			p.query.Fields = append(p.query.Fields, identifier)
			p.pop(leng)
			maybeFrom, leng := p.getToken()
			if strings.ToUpper(maybeFrom) == "FROM" {
				p.step = stepSelectFrom
				continue
			}
			p.step = stepSelectComma
		case stepSelectComma:
			token, leng := p.getToken()
			if token != "," {
				return p.query, fmt.Errorf("at SELECT: expected comma or FROM")
			}
			p.pop(leng)
			p.step = stepAggregateFunc
		case stepSelectFrom:
			token, leng := p.getToken()
			if strings.ToUpper(token) != "FROM" {
				return p.query, fmt.Errorf("at SELECT: expected FROM")
			}
			p.pop(leng)
			p.step = stepSelectFromTable
		case stepSelectFromTable:
			token, leng := p.getToken()
			if len(token) == 0 {
				return p.query, fmt.Errorf("at SELECT: expected quoted table name")
			}
			p.query.TableName = token
			p.pop(leng)
			p.step = stepWhere
		case stepInsertTable:
			token, leng := p.getToken()
			if len(token) == 0 {
				return p.query, fmt.Errorf("at INSERT INTO: expected quoted table name")
			}
			p.query.TableName = token
			p.pop(leng)
			p.step = stepInsertFieldsOpeningParens
		case stepDeleteFromTable:
			token, leng := p.getToken()
			if len(token) == 0 {
				return p.query, fmt.Errorf("at DELETE FROM: expected quoted table name")
			}
			p.query.TableName = token
			p.pop(leng)
			p.step = stepWhere
		case stepUpdateTable:
			token, leng := p.getToken()
			if len(token) == 0 {
				return p.query, fmt.Errorf("at UPDATE: expected quoted table name")
			}
			p.query.TableName = token
			p.pop(leng)
			p.step = stepUpdateSet
		case stepUpdateSet:
			token, leng := p.getToken()
			if token != "SET" {
				return p.query, fmt.Errorf("at UPDATE: expected 'SET'")
			}
			p.pop(leng)
			p.step = stepUpdateField
		case stepUpdateField:
			token, leng := p.getToken()
			if !isIdentifier(token) {
				return p.query, fmt.Errorf("at UPDATE: expected at least one field to update")
			}
			p.nextUpdateField = token
			p.pop(leng)
			p.step = stepUpdateEquals
		case stepUpdateEquals:
			token, leng := p.getToken()
			if token != "=" {
				return p.query, fmt.Errorf("at UPDATE: expected '='")
			}
			p.pop(leng)
			p.step = stepUpdateValue
		case stepUpdateValue:
			value, leng := p.getValueWithLength()
			if leng == 0 {
				return p.query, fmt.Errorf("at UPDATE: expected quoted value")
			}
			p.query.Updates[p.nextUpdateField] = value
			p.nextUpdateField = ""
			p.pop(leng)
			token, _ := p.getToken()
			if strings.ToUpper(token) == "WHERE" {
				p.step = stepWhere
				continue
			}
			p.step = stepUpdateComma
		case stepUpdateComma:
			token, leng := p.getToken()
			if token != "," {
				return p.query, fmt.Errorf("at UPDATE: expected ','")
			}
			p.pop(leng)
			p.step = stepUpdateField
		case stepWhere:
			token, leng := p.getToken()
			if strings.ToUpper(token) != "WHERE" {
				return p.query, fmt.Errorf("expected WHERE")
			}
			p.pop(leng)
			p.step = stepWhereField
		case stepWhereField:
			token, leng := p.getToken()
			if !isIdentifier(token) {
				return p.query, fmt.Errorf("at WHERE: expected field")
			}
			p.query.Conditions = append(p.query.Conditions, Condition{Operand1: token, Operand1IsField: true})
			p.pop(leng)
			p.step = stepWhereOperator
		case stepWhereOperator:
			token, leng := p.getToken()
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			switch token {
			case "=":
				currentCondition.Operator = Eq
			case ">":
				currentCondition.Operator = Gt
			case ">=":
				currentCondition.Operator = Gte
			case "<":
				currentCondition.Operator = Lt
			case "<=":
				currentCondition.Operator = Lte
			case "!=":
				currentCondition.Operator = Ne
			default:
				return p.query, fmt.Errorf("at WHERE: unknown operator")
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop(leng)
			p.step = stepWhereValue
		case stepWhereValue:
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			token, leng := p.getToken()
			if isIdentifier(token) {
				currentCondition.Operand2 = token
				currentCondition.Operand2IsField = true
			} else {
				quotedValue, ln := p.getValueWithLength()
				if ln == 0 {
					return p.query, fmt.Errorf("at WHERE: expected quoted value")
				}
				currentCondition.Operand2 = quotedValue
				currentCondition.Operand2IsField = false
				leng = ln
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop(leng)
			token, _ = p.getToken()
			if token == "AND" {
				p.step = stepWhereAnd
			} else {
				p.step = stepWhereOr
			}
		case stepWhereAnd:
			token, leng := p.getToken()
			if strings.ToUpper(token) != "AND" {
				return p.query, fmt.Errorf("expected AND/OR")
			}
			p.query.ConditionOperators = append(p.query.ConditionOperators, "AND")
			p.pop(leng)
			p.step = stepWhereField
		case stepWhereOr:
			token, leng := p.getToken()
			if strings.ToUpper(token) != "OR" {
				return p.query, fmt.Errorf("expected AND/OR")
			}
			p.query.ConditionOperators = append(p.query.ConditionOperators, "OR")
			p.pop(leng)
			p.step = stepWhereField
		case stepInsertFieldsOpeningParens:
			token, leng := p.getToken()
			if len(token) != 1 || token != "(" {
				return p.query, fmt.Errorf("at INSERT INTO %s: expected opening parens", p.query.TableName)
			}
			p.pop(leng)
			p.step = stepInsertFields
		case stepInsertFields:
			token, leng := p.getToken()
			if !isIdentifier(token) {
				return p.query, fmt.Errorf("at INSERT INTO %s: expected at least one field to insert", p.query.TableName)
			}
			p.query.Fields = append(p.query.Fields, token)
			p.pop(leng)
			p.step = stepInsertFieldsCommaOrClosingParens
		case stepInsertFieldsCommaOrClosingParens:
			token, leng := p.getToken()
			if token != "," && token != ")" {
				return p.query, fmt.Errorf("at INSERT INTO %s: expected comma or closing parens", p.query.TableName)
			}
			p.pop(leng)
			if token == "," {
				p.step = stepInsertFields
				continue
			}
			p.step = stepInsertValuesRWord
		case stepInsertValuesRWord:
			token, leng := p.getToken()
			if strings.ToUpper(token) != "VALUES" {
				return p.query, fmt.Errorf("at INSERT INTO: expected 'VALUES'")
			}
			p.pop(leng)
			p.step = stepInsertValuesOpeningParens
		case stepInsertValuesOpeningParens:
			token, leng := p.getToken()
			if token != "(" {
				return p.query, fmt.Errorf("at INSERT INTO: expected opening parens")
			}
			p.query.Inserts = append(p.query.Inserts, []string{})
			p.pop(leng)
			p.step = stepInsertValues
		case stepInsertValues:
			quotedValue, leng := p.getValueWithLength()
			if leng == 0 {
				return p.query, fmt.Errorf("at INSERT INTO: expected quoted value")
			}
			p.query.Inserts[len(p.query.Inserts)-1] = append(p.query.Inserts[len(p.query.Inserts)-1], quotedValue)
			p.pop(leng)
			p.step = stepInsertValuesCommaOrClosingParens
		case stepInsertValuesCommaOrClosingParens:
			token, leng := p.getToken()
			if token != "," && token != ")" {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
			}
			p.pop(leng)
			if token == "," {
				p.step = stepInsertValues
				continue
			}
			currentInsertRow := p.query.Inserts[len(p.query.Inserts)-1]
			if len(currentInsertRow) < len(p.query.Fields) {
				return p.query, fmt.Errorf("at INSERT INTO: value count doesn't match field count")
			}
			p.step = stepInsertValuesCommaBeforeOpeningParens
		case stepInsertValuesCommaBeforeOpeningParens:
			token, leng := p.getToken()
			if strings.ToUpper(token) != "," {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma")
			}
			p.pop(leng)
			p.step = stepInsertValuesOpeningParens
		}
	}
}

func (p *parser) getToken() (string, int) {
	peeked, leng := p.peekWithLength()
	return peeked, leng
}

func (p *parser) peekWithLength() (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}

	for _, rWord := range reservedWords {
		token := strings.ToUpper(p.sql[p.i:getMin(len(p.sql), p.i+len(rWord))])
		if token == rWord {
			return token, len(token)
		}
	}

	return p.peekIdentifierWithLength()
}

func (p *parser) peekIdentifierWithLength() (string, int) {
	for i := p.i; i < len(p.sql); i++ {
		if matched, _ := regexp.MatchString(`[a-zA-Z0-9_*]`, string(p.sql[i])); !matched {
			return p.sql[p.i:i], len(p.sql[p.i:i])
		}
	}
	return p.sql[p.i:], len(p.sql[p.i:])
}

func (p *parser) getValueWithLength() (string, int) {
	if len(p.sql) < p.i || p.sql[p.i] != '\'' {
		return "", 0
	}
	for i := p.i + 1; i < len(p.sql); i++ {
		if p.sql[i] == '\'' && p.sql[i-1] != '\\' {
			return p.sql[p.i+1 : i], len(p.sql[p.i+1:i]) + 2 // +2 for the two quotes
		}
	}
	return "", 0
}

func (p *parser) getAggToken() (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}

	for _, agg := range aggFunc {
		token := strings.ToUpper(p.sql[p.i:getMin(len(p.sql), p.i+len(agg))])
		if token == agg {
			return token, len(token)
		}
	}
	return "", 0
}

func (p *parser) pop(i int) {
	// if p.i >= len(p.sql) {
	// 	return
	// }

	p.i = p.i + i

	for ; p.i < len(p.sql) && p.sql[p.i] == ' '; p.i++ {
	}
}

func (p *parser) validate() error {
	if len(p.query.Conditions) == 0 && p.step == stepWhereField {
		return fmt.Errorf("at WHERE: empty WHERE clause")
	}

	if (p.step == stepWhereAnd || p.step == stepWhereOr) && p.i < len(p.sql) {
		return fmt.Errorf("at WHERE: no condition after AND/OR")
	}
	if p.query.Type == UnknownType {
		return fmt.Errorf("query type cannot be empty")
	}
	if p.query.TableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(p.query.Conditions) == 0 && (p.query.Type == Update || p.query.Type == Delete) {
		return fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE")
	}
	for _, c := range p.query.Conditions {
		if c.Operator == UnknownOperator {
			return fmt.Errorf("at WHERE: condition without operator")
		}
		if c.Operand1 == "" && c.Operand1IsField {
			return fmt.Errorf("at WHERE: condition with empty left side operand")
		}
		if c.Operand2 == "" && c.Operand2IsField {
			return fmt.Errorf("at WHERE: condition with empty right side operand")
		}
	}
	if p.query.Type == Insert && len(p.query.Inserts) == 0 {
		return fmt.Errorf("at INSERT INTO: need at least one row to insert")
	}
	if p.query.Type == Insert {
		for _, i := range p.query.Inserts {
			if len(i) != len(p.query.Fields) {
				return fmt.Errorf("at INSERT INTO: value count doesn't match field count")
			}
		}
	}
	return nil
}

func (p *parser) logError() {
	if p.err == nil {
		return
	}
	fmt.Println(p.sql)
	fmt.Println(strings.Repeat(" ", p.i) + "^")
	fmt.Println(p.err)
}

func isIdentifier(s string) bool {
	for _, rw := range reservedWords {
		if strings.ToUpper(s) == rw {
			return false
		}
	}
	matched, _ := regexp.MatchString("[a-zA-Z_][a-zA-Z_0-9]*", s)
	return matched
}

func isIdentifierOrAsterisk(s string) bool {
	return isIdentifier(s) || s == "*"
}

func getMin(a, b int) int {
	if a > b {
		return b
	}
	return a
}
