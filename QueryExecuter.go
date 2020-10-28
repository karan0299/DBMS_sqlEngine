package main

import (
	"fmt"
	"sqlengine/parser"
)

func (d *Database) executeQuery(q parser.Query) {
	switch q.Type {
	case 1:
		d.selectExecuter(q, d.tables[q.TableName])
	case 3:
		d.insertExecuter(q, d.tables[q.TableName])
	case 5:
		d.AddTable(q)
	default:
		fmt.Println("Wrong Query")
	}
}

func (t *Table) checkCondition(c parser.Condition, r *Row) bool {
	var op1 string
	var op2 string
	if c.Operand1IsField {
		op1 = r.data[t.index[c.Operand1]-1]
	} else {
		op1 = c.Operand1
	}
	if c.Operand2IsField {
		op2 = r.data[t.index[c.Operand2]-1]
	} else {
		op2 = c.Operand2
	}
	// fmt.Println(op1, " op2 ", op2, c.Operator)
	switch c.Operator {
	case 1:
		return op1 == op2
	case 2:
		return op1 != op2
	case 3:
		return op1 > op2
	case 4:
		return op1 < op2
	case 5:
		return op1 >= op2
	case 6:
		return op1 <= op2
	default:
		return false
	}
}

func (d *Database) selectExecuter(q parser.Query, t *Table) {
	if q.Fields[0] == "*" {
		q.Fields = t.columns
	}
	if len(q.AggregateFunc) == 0 {
		t1 := d.makeTable(q)
		for current := t.rowhead; current != nil; current = current.next {
			var satisfied bool = true
			for i := 0; i < len(q.Conditions); i++ {
				if i == 0 {
					satisfied = (satisfied && t.checkCondition(q.Conditions[i], current))
				} else {
					if q.ConditionOperators[i-1] == "AND" {
						satisfied = satisfied && t.checkCondition(q.Conditions[i], current)
					} else {
						satisfied = satisfied || t.checkCondition(q.Conditions[i], current)
					}
				}

			}
			if satisfied {
				var data []string
				for j := 0; j < len(q.Fields); j++ {
					data = append(data, current.data[t.index[q.Fields[j]]-1])
				}
				t1.addSingleRow(data, t1.columns)
			}
		}
		d.printTable(t1)

	}
	// d.printTable(t)
}
func (d *Database) insertExecuter(q parser.Query, t *Table) {
	t.addRow(q.Inserts, q.Fields)
}
