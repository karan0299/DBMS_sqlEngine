package main

import (
	"fmt"
	"sqlengine/parser"
)

type mapKey struct {
	groupByFields string
}
type mapValue struct {
	count  int
	rowptr *Row
}

func (d *Database) executeQuery(q parser.Query) {
	switch q.Type {
	case 1:
		d.selectExecuter(q, d.tables[q.TableName])
	case 2:
		d.updateExecuter(q, d.tables[q.TableName])
	case 3:
		d.insertExecuter(q, d.tables[q.TableName])
	case 4:
		d.deleteExecuter(q, d.tables[q.TableName])
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
	if len(q.GroupByField) == 0 {
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
				t1.addSingleRowNoGroupBy(data, t1.columns)
			}
		}
		d.printTable(t1)

	} else {
		t1 := d.makeTableAggFunc(q)
		mpKey := mapKey{
			groupByFields: "(",
		}
		mpValue := mapValue{
			count:  0,
			rowptr: nil,
		}
		mp := map[mapKey]mapValue{}
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
				mpKey.groupByFields = "("
				for i := 0; i < len(q.GroupByField); i++ {
					mpKey.groupByFields += current.data[t.index[q.GroupByField[i]]-1] + ","
				}
				mpKey.groupByFields += ")"
				if mp[mpKey].count == 0 {
					// fmt.Println("Not Found", current.data)
					mpValue.count = 1
					t1.addSingleRowAggFunc(current.data, t, q)
					mpValue.rowptr = t1.rowtail
					mp[mpKey] = mpValue
				} else {
					// fmt.Println("Found", current.data)
					mpValue.count = mp[mpKey].count + 1
					mpValue.rowptr = mp[mpKey].rowptr
					mp[mpKey] = mpValue
					t1.alterRowAggFunc(current.data, t, q, mpValue.rowptr, mpValue.count)
				}
				// fmt.Println(mp)
				// d.printTable(t1)
			}
		}

		d.printTable(t1)

	}
	// d.printTable(t)
}
func (d *Database) insertExecuter(q parser.Query, t *Table) {
	t.addRow(q.Inserts, q.Fields)
}
func (d *Database) updateExecuter(q parser.Query, t *Table) {
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
			for field, value := range q.Updates {
				current.data[t.index[field]-1] = value
			}
		}
	}
}
func (d *Database) deleteExecuter(q parser.Query, t *Table) {
	var newRowHead *Row
	var newRowTail *Row
	newRowHead = nil
	newRowTail = nil
	for current := t.rowhead; current != nil; current = current.next {
		var satisfied bool = false
		for i := 0; i < len(q.Conditions); i++ {
			if i == 0 {
				satisfied = (satisfied || t.checkCondition(q.Conditions[i], current))
			} else {
				if q.ConditionOperators[i-1] == "AND" {
					satisfied = satisfied && t.checkCondition(q.Conditions[i], current)
				} else {
					satisfied = satisfied || t.checkCondition(q.Conditions[i], current)
				}
			}

		}
		if !satisfied {
			if newRowHead == nil {
				newRowHead = current
				newRowTail = current
			} else {
				newRowTail.next = current
				newRowTail = newRowTail.next
			}
		}
	}
	newRowTail.next = nil
	t.rowhead = newRowHead
	t.rowtail = newRowTail
}
