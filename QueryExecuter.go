package main

import (
	"sqlengine/parser"
	"strconv"

	"gopkg.in/src-d/go-errors.v1"
)

type mapKey struct {
	groupByFields string
}
type mapValue struct {
	count  int
	rowptr *Row
}

func tableToStruct(t *Table) *rowColResult {
	// r:= &rowColResult{
	// 	Cols: make([]string, len(t.columns)),
	// 	Data: nil,
	// }
	// for i:=0;i<len(t.columns);i++{

	// }
	r := &rowColResult{}
	r.Cols = t.columns
	for current := t.rowhead; current != nil; current = current.next {
		r.Data = append(r.Data, current.data)
	}
	return r
}

func (d *Database) executeQuery(q parser.Query) (*rowColResult, error) {
	var t *Table
	var e error
	switch q.Type {
	case 1:
		t, e = d.selectExecuter(q, d.tables[q.TableName])
	case 2:
		t, e = nil, d.updateExecuter(q, d.tables[q.TableName])
	case 3:
		t, e = nil, d.insertExecuter(q, d.tables[q.TableName])
	case 4:
		t, e = nil, d.deleteExecuter(q, d.tables[q.TableName])
	// case 5:
	// return d.AddTable(q)
	default:
		t, e = nil, ErrWrongQuery.New()
	}
	if e != nil {
		return nil, e
	} else {
		return tableToStruct(t), e
	}
}

func (t *Table) checkCondition(c parser.Condition, r *Row) (bool, error) {
	var val1 string
	var val2 string
	if c.Operand1IsField {
		if t.index[c.Operand1] == 0 {
			return false, ErrColumnNotFound.New(c.Operand1)
		}
		val1 = r.data[t.index[c.Operand1]-1]
	} else {
		val1 = c.Operand1
	}
	if c.Operand2IsField {
		if t.index[c.Operand2] == 0 {
			return false, ErrColumnNotFound.New(c.Operand2)
		}
		val2 = r.data[t.index[c.Operand2]-1]
	} else {
		val2 = c.Operand2
	}
	op1, err1 := strconv.ParseFloat(val1, 64)
	op2, err2 := strconv.ParseFloat(val2, 64)
	if err1 != nil || err2 != nil {
		return false, ErrNonNumericValue.New()
	}
	// fmt.Println(op1, " op2 ", op2, c.Operator)
	switch c.Operator {
	case 1:
		return op1 == op2, nil
	case 2:
		return op1 != op2, nil
	case 3:
		return op1 > op2, nil
	case 4:
		return op1 < op2, nil
	case 5:
		return op1 >= op2, nil
	case 6:
		return op1 <= op2, nil
	default:
		return false, nil
	}
}
func (t *Table) checkHavingCondition(c parser.HavingCondition, r *Row) (bool, error) {
	var val1 string
	var val2 string
	temp := c.OperandAggFunc + "(" + c.OperandField1 + ")"
	if t.index[temp] == 0 {
		return false, ErrColumnNotFound.New(temp)
	}
	val1 = r.data[t.index[temp]-1]
	val2 = c.Operand2
	op1, err1 := strconv.ParseFloat(val1, 64)
	op2, err2 := strconv.ParseFloat(val2, 64)
	if err1 != nil {
		return false, ErrNonNumericValue.New()
	}
	if err2 != nil {
		return false, ErrNonNumericValue.New()
	}
	switch c.Operator {
	case 1:
		return op1 == op2, nil
	case 2:
		return op1 != op2, nil
	case 3:
		return op1 > op2, nil
	case 4:
		return op1 < op2, nil
	case 5:
		return op1 >= op2, nil
	case 6:
		return op1 <= op2, nil
	default:
		return false, nil
	}
}

func (d *Database) selectExecuter(q parser.Query, t *Table) (*Table, error) {
	if q.Fields[0] == "*" {
		q.Fields = t.columns
	}
	if len(q.GroupByField) == 0 {
		t1 := d.makeTable(q)
		for current := t.rowhead; current != nil; current = current.next {
			var satisfied bool = true
			for i := 0; i < len(q.Conditions); i++ {
				tempSatisfied, err := t.checkCondition(q.Conditions[i], current)
				if err != nil {
					return nil, err
				}
				if i == 0 {
					satisfied = (satisfied && tempSatisfied)
				} else {
					if q.ConditionOperators[i-1] == "AND" {
						satisfied = satisfied && tempSatisfied
					} else {
						satisfied = satisfied || tempSatisfied
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
		return t1, nil

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
				tempSatisfied, err := t.checkCondition(q.Conditions[i], current)
				if err != nil {
					return nil, err
				}
				if i == 0 {
					satisfied = (satisfied && tempSatisfied)
				} else {
					if q.ConditionOperators[i-1] == "AND" {
						satisfied = satisfied && tempSatisfied
					} else {
						satisfied = satisfied || tempSatisfied
					}
				}

			}
			if satisfied {
				mpKey.groupByFields = "("
				for i := 0; i < len(q.GroupByField); i++ {
					if t.index[q.GroupByField[i]] == 0 {
						return nil, ErrColumnNotFound.New(q.GroupByField[i])
					}
					mpKey.groupByFields += current.data[t.index[q.GroupByField[i]]-1] + ","
				}
				mpKey.groupByFields += ")"
				if mp[mpKey].count == 0 {
					// fmt.Println("Not Found", current.data)
					mpValue.count = 1
					err := t1.addSingleRowAggFunc(current.data, t, q)
					if err != nil {
						return nil, err
					}
					mpValue.rowptr = t1.rowtail
					mp[mpKey] = mpValue
				} else {
					// fmt.Println("Found", current.data)
					mpValue.count = mp[mpKey].count + 1
					mpValue.rowptr = mp[mpKey].rowptr
					mp[mpKey] = mpValue
					err := t1.alterRowAggFunc(current.data, t, q, mpValue.rowptr, mpValue.count)
					if err != nil {
						return nil, err
					}
				}
				// fmt.Println(mp)
				// d.printTable(t1)
			}
		}
		if len(q.HavingConditions) != 0 {
			t2 := d.makeTableAggFunc(q)
			var satisfied bool = true
			for current := t1.rowhead; current != nil; current = current.next {
				for i := 0; i < len(q.HavingConditions); i++ {
					tempSatisfied, err := t1.checkHavingCondition(q.HavingConditions[i], current)
					if err != nil {
						return nil, err
					}
					if i == 0 {
						satisfied = (satisfied && tempSatisfied)
					} else {
						if q.HavingConditionsOperators[i-1] == "AND" {
							satisfied = satisfied && tempSatisfied
						} else {
							satisfied = satisfied || tempSatisfied
						}
					}
				}
				if satisfied {
					if t2.rowhead == nil {
						t2.rowhead = current
						t2.rowtail = t2.rowhead
					} else {
						t2.rowtail.next = current
						t2.rowtail = t2.rowtail.next
					}
				}
			}
			if t2.rowtail != nil {
				t2.rowtail.next = nil
			}
			t1 = t2
		}
		return t1, nil

	}
	// d.printTable(t)
}
func (d *Database) insertExecuter(q parser.Query, t *Table) error {
	err := t.addRow(q.Inserts, q.Fields)
	if err != nil {
		return err
	}
	return errors.NewKind("Rows inserted successfully").New()
}
func (d *Database) updateExecuter(q parser.Query, t *Table) error {
	for current := t.rowhead; current != nil; current = current.next {
		var satisfied bool = true
		for i := 0; i < len(q.Conditions); i++ {
			tempSatisfied, err := t.checkCondition(q.Conditions[i], current)
			if err != nil {
				return err
			}
			if i == 0 {
				satisfied = (satisfied && tempSatisfied)
			} else {
				if q.ConditionOperators[i-1] == "AND" {
					satisfied = satisfied && tempSatisfied
				} else {
					satisfied = satisfied || tempSatisfied
				}
			}

		}
		if satisfied {
			for field, value := range q.Updates {
				if t.index[field] == 0 {
					return ErrColumnNotFound.New(field)
				}
				current.data[t.index[field]-1] = value
			}
		}
	}
	return errors.NewKind("Rows updated successfully").New()
}
func (d *Database) deleteExecuter(q parser.Query, t *Table) error {
	var newRowHead *Row
	var newRowTail *Row
	newRowHead = nil
	newRowTail = nil
	for current := t.rowhead; current != nil; current = current.next {
		var satisfied bool = false
		for i := 0; i < len(q.Conditions); i++ {
			tempSatisfied, err := t.checkCondition(q.Conditions[i], current)
			if err != nil {
				return err
			}
			if i == 0 {
				satisfied = (satisfied || tempSatisfied)
			} else {
				if q.ConditionOperators[i-1] == "AND" {
					satisfied = satisfied && tempSatisfied
				} else {
					satisfied = satisfied || tempSatisfied
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
	return errors.NewKind("Rows deleted succesfully").New()
}
