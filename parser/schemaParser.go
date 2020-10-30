package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// SchemaParse parses the given schema
func SchemaParse(sqlq string) (Schema, error) {
	parserobj := &schemaParser{
		i:      0,
		sql:    sqlq,
		step:   stepCreate,
		schema: Schema{},
		err:    nil,
	}

	parserobj.schema.Columns = map[string]DataType{}

	return parserobj.parse()
}

func (p *schemaParser) parse() (Schema, error) {
	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	p.logError()
	return q, p.err
}

func (p *schemaParser) doParse() (Schema, error) {
	for {
		if p.i >= len(p.sql) {
			return p.schema, p.err
		}
		switch p.step {
		case stepCreate:
			token, leng := p.getToken()
			if strings.ToUpper(token) != "CREATE" {
				return p.schema, fmt.Errorf("Expected CREATE: found %s", token)
			}
			p.pop(leng)
			p.step = stepCreateTable
		case stepCreateTable:
			token, leng := p.getToken()
			if strings.ToUpper(token) != "TABLE" {
				return p.schema, fmt.Errorf("at CREATE : Expected TABLE , found %s", token)
			}
			p.pop(leng)
			p.step = stepTableName
		case stepTableName:
			token, leng := p.getToken()
			if !p.isIdentifier(token) || leng == 0 {
				return p.schema, fmt.Errorf("at CREATE : Expected table name string ")
			}
			p.pop(leng)
			p.schema.TableName = token
			p.step = stepCreateOpenParens
		case stepCreateOpenParens:
			token, leng := p.getToken()
			if len(token) != 1 || token != "(" {
				return p.schema, fmt.Errorf("at CREATE : expected opening parenthesis")
			}
			p.pop(leng)
			p.step = stepTableColumn
		case stepTableColumn:
			identifier, leng := p.getToken()
			if !p.isIdentifier(identifier) {
				return p.schema, fmt.Errorf("at CREATE: expected field to create")
			}
			p.currentField = identifier
			p.pop(leng)
			p.step = stepColumnType
		case stepColumnType:
			token, leng := p.getToken()
			if !p.isDataType(token) {
				return p.schema, fmt.Errorf("at CREATE: expected data type")
			}
			p.currentDataType = DataType{
				Dtype: token,
			}
			p.pop(leng)
			p.step = stepColumnTypeOpenParens
		case stepColumnTypeOpenParens:
			token, leng := p.getToken()
			if len(token) != 1 || token != "(" {
				return p.schema, fmt.Errorf("at CREATE : expected opening parenthesis for defining size")
			}
			p.pop(leng)
			p.step = stepColumnTypeSize
		case stepColumnTypeSize:
			token, leng := p.getToken()
			if !isNumber(token) {
				return p.schema, fmt.Errorf("at CREATE : expected number in size")
			}
			p.currentDataType.Size, _ = strconv.Atoi(token)
			p.schema.Columns[p.currentField] = p.currentDataType
			p.pop(leng)
			p.step = stepColumnTypeCloseParens
		case stepColumnTypeCloseParens:
			token, leng := p.getToken()
			if len(token) != 1 || token != ")" {
				return p.schema, fmt.Errorf("at CREATE : expected closing parenthesis here")
			}
			p.pop(leng)
			maybeParens, _ := p.getToken()
			if maybeParens == ")" {
				p.step = stepCreateCloseParens
			} else {
				p.step = stepColumnComma
			}
		case stepColumnComma:
			token, leng := p.getToken()
			if token != "," {
				return p.schema, fmt.Errorf("at CREATE: expected comma or closing parenthesis")
			}
			p.pop(leng)
			p.step = stepTableColumn
		case stepCreateCloseParens:
			token, leng := p.getToken()
			if token != ")" {
				return p.schema, fmt.Errorf("at CREATE: expected closing parenthesis")
			}
			p.pop(leng)
			p.step = stepEnd
			if token, _ := p.getToken(); token != "" {
				return p.schema, fmt.Errorf("at CREATE: expected no token but found %s", token)
			}
		}
	}
}

func (p *schemaParser) getToken() (string, int) {
	peeked, leng := p.peekWithLength()
	return peeked, leng
}

func (p *schemaParser) peekWithLength() (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}

	for _, rWord := range schemaReserveWords {
		token := strings.ToUpper(p.sql[p.i:getMin(len(p.sql), p.i+len(rWord))])
		if token == rWord {
			return token, len(token)
		}
	}

	for _, dtype := range dataTypes {
		token := p.sql[p.i:getMin(len(p.sql), p.i+len(dtype))]
		if token == dtype {
			return token, len(token)
		}
	}
	return p.peekIdentifierWithLength()
}

func (p *schemaParser) peekIdentifierWithLength() (string, int) {
	for i := p.i; i < len(p.sql); i++ {
		if matched, _ := regexp.MatchString(`[a-zA-Z0-9_*]`, string(p.sql[i])); !matched {
			return p.sql[p.i:i], len(p.sql[p.i:i])
		}
	}
	return p.sql[p.i:], len(p.sql[p.i:])
}

func (p *schemaParser) pop(i int) {
	p.i = p.i + i

	for ; p.i < len(p.sql) && p.sql[p.i] == ' '; p.i++ {
	}
}

func (*schemaParser) isIdentifier(s string) bool {
	for _, srw := range schemaReserveWords {
		if s == srw {
			return false
		}
	}

	for _, dt := range dataTypes {
		if s == dt {
			return false
		}
	}

	matched, _ := regexp.MatchString("[a-zA-Z_][a-zA-Z_0-9]*", s)
	return matched
}

func (*schemaParser) isDataType(s string) bool {
	for _, dt := range dataTypes {
		if s == dt {
			return true
		}
	}

	return false
}

func (p *schemaParser) logError() {
	if p.err == nil {
		return
	}
	fmt.Println(p.sql)
	fmt.Println(strings.Repeat(" ", p.i) + "^")
	fmt.Println(p.err)
}

func (p *schemaParser) validate() error {
	if p.i >= len(p.sql) && p.step != stepEnd {
		return fmt.Errorf("at Create: incomplete schema")
	}
	return nil
}
