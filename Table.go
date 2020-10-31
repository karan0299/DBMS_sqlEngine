package main

import (
	"fmt"
	"sqlengine/parser"
	"strconv"
)

type Row struct {
	data []string
	next *Row
}

func (t *Table) addSingleRow(data []string, Fields []string) error {
	if t.rowhead == nil {
		t.rowhead = &Row{
			data: make([]string, len(t.columns)),
			next: nil,
		}
		t.rowtail = t.rowhead
	} else {
		t.rowtail.next = &Row{
			data: make([]string, len(t.columns)),
			next: nil,
		}
		t.rowtail = t.rowtail.next
	}
	// for i := 0; i < len(t.columns); i++ {
	// 	t.rowhead.data[i] = "NULL"
	// }
	for i := 0; i < len(Fields); i++ {
		if t.index[Fields[i]] == 0 {
			return ErrColumnNotFound.New(Fields[i])
		}
		t.rowtail.data[t.index[Fields[i]]-1] = data[i]
	}
	return nil
}
func (t *Table) addSingleRowNoGroupBy(data []string, Fields []string) {
	if t.rowhead == nil {
		t.rowhead = &Row{
			data: make([]string, len(t.columns)),
			next: nil,
		}
		t.rowtail = t.rowhead
	} else {
		t.rowtail.next = &Row{
			data: make([]string, len(t.columns)),
			next: nil,
		}
		t.rowtail = t.rowtail.next
	}
	// for i := 0; i < len(t.columns); i++ {
	// 	t.rowhead.data[i] = "NULL"
	// }
	for i := 0; i < len(Fields); i++ {
		t.rowtail.data[i] = data[i]
	}

}
func (t *Table) addSingleRowAggFunc(data []string, t1 *Table, q parser.Query) error {
	if t.rowhead == nil {
		t.rowhead = &Row{
			data: make([]string, len(t.columns)),
			next: nil,
		}
		t.rowtail = t.rowhead
	} else {
		t.rowtail.next = &Row{
			data: make([]string, len(t.columns)),
			next: nil,
		}
		t.rowtail = t.rowtail.next
	}
	// fmt.Println(t)
	// for i := 0; i < len(t.columns); i++ {
	// 	t.rowhead.data[i] = "NULL"
	// }
	// for i := 0; i < len(t.columns); i++ {
	// t.rowtail.data[t.index[Fields[i]]-1] = data[i]
	j := 0
	// for k:=0;k<len(q.GroupByField);k++{
	// 	for l:=0;l<len(q.GroupByField[k]);l++{
	// 		t.rowtail.data[j]=
	// 	}
	// }
	for k := 0; k < len(q.AggregateFunc["COUNT"]); k++ {
		if t1.index[q.AggregateFunc["COUNT"][k]] == 0 {
			return ErrColumnNotFound.New(q.AggregateFunc["COUNT"][k])
		}
		t.rowtail.data[j] = "1"
		j++
	}
	for k := 0; k < len(q.AggregateFunc["SUM"]); k++ {
		if t1.index[q.AggregateFunc["SUM"][k]] == 0 {
			return ErrColumnNotFound.New(q.AggregateFunc["SUM"][k])
		}
		t.rowtail.data[j] = data[t1.index[q.AggregateFunc["SUM"][k]]-1]
		j++
	}
	for k := 0; k < len(q.AggregateFunc["AVG"]); k++ {
		if t1.index[q.AggregateFunc["AVG"][k]] == 0 {
			return ErrColumnNotFound.New(q.AggregateFunc["AVG"][k])
		}
		t.rowtail.data[j] = data[t1.index[q.AggregateFunc["AVG"][k]]-1]
		j++
	}

	// for FUNC, FIELDS := range q.AggregateFunc {
	// 	fmt.Println(FUNC)
	// 	for k := 0; k < len(FIELDS); k++ {
	// 		switch FUNC {
	// 		case "COUNT":
	// 			t.rowtail.data[j] = "1"
	// 		case "SUM":
	// 			t.rowtail.data[j] = data[t1.index[FIELDS[k]]-1]
	// 		case "AVG":
	// 			t.rowtail.data[j] = data[t1.index[FIELDS[k]]-1]
	// 		}
	// 		j++
	// 	}
	// }
	// fmt.Println(t)
	// fmt.Println(j)
	// fmt.Println(t.rowhead.data)
	for j < len(t.columns) {
		if t1.index[t.columns[j]] == 0 {
			return ErrColumnNotFound.New(t.columns[j])
		}
		t.rowtail.data[j] = data[t1.index[t.columns[j]]-1]
		j++
	}
	// fmt.Println("F table data", data)
	// fmt.Println("F outpu data", t.rowtail.data)
	// }
	return nil
}
func (t *Table) alterRowAggFunc(data []string, t1 *Table, q parser.Query, rowptr *Row, count int) error {
	j := 0

	for k := 0; k < len(q.AggregateFunc["COUNT"]); k++ {
		a, err := strconv.ParseInt(rowptr.data[j], 10, 64)
		if err == nil {
			a = a + 1
			rowptr.data[j] = strconv.FormatInt(a, 10)
		} else {
			return ErrNonNumericValue.New()
		}
		j++
	}
	for k := 0; k < len(q.AggregateFunc["SUM"]); k++ {
		a, err := strconv.ParseFloat(rowptr.data[j], 64)
		if err == nil {
			b, err1 := strconv.ParseFloat(data[t1.index[q.AggregateFunc["SUM"][k]]-1], 64)
			if err1 == nil {
				a = a + b
				rowptr.data[j] = strconv.FormatFloat(a, 'f', 3, 64)
			} else {
				return ErrNonNumericValue.New()
			}
		} else {
			return ErrNonNumericValue.New()
		}
		j++
	}
	for k := 0; k < len(q.AggregateFunc["AVG"]); k++ {
		a, err := strconv.ParseFloat(rowptr.data[j], 64)
		a = a * float64((count - 1))
		if err == nil {
			b, err1 := strconv.ParseFloat(data[t1.index[q.AggregateFunc["AVG"][k]]-1], 64)
			if err1 == nil {
				a = a + b
				a = a / float64(count)
				rowptr.data[j] = strconv.FormatFloat(a, 'f', 3, 64)
			} else {
				return ErrNonNumericValue.New()
			}
		} else {
			return ErrNonNumericValue.New()
		}
		j++
	}
	return nil
}

// for FUNC, FIELDS := range q.AggregateFunc {
// 	for k := 0; k < len(FIELDS); k++ {
// 		switch FUNC {
// 		case "COUNT":
// 			a, err := strconv.ParseFloat(rowptr.data[j], 64)
// 			if err == nil {
// 				a = a + 1
// 				rowptr.data[j] = strconv.FormatFloat(a, 'f', 3, 64)
// 			}
// 		case "SUM":
// 			a, err := strconv.ParseFloat(rowptr.data[j], 64)
// 			if err == nil {
// 				b, err1 := strconv.ParseFloat(data[t1.index[FIELDS[k]]-1], 64)
// 				if err1 == nil {
// 					a = a + b
// 					rowptr.data[j] = strconv.FormatFloat(a, 'f', 3, 64)
// 				} else {
// 					fmt.Println("This field has non numeric values") //Put error
// 				}
// 			}

// 		case "AVG":

// 			a, err := strconv.ParseFloat(rowptr.data[j], 64)
// 			a = a * float64((count - 1))
// 			if err == nil {
// 				b, err1 := strconv.ParseFloat(data[t1.index[FIELDS[k]]-1], 64)
// 				if err1 == nil {
// 					a = a + b
// 					a = a / float64(count)
// 					rowptr.data[j] = strconv.FormatFloat(a, 'f', 3, 64)
// 				} else {
// 					fmt.Println("This field has non numeric values") //Put error
// 				}
// 			}

// 		}
// 		fmt.Println("table data", data)
// 		fmt.Println("outpu data", rowptr.data)
// 		j++
// 	}

// }
// fmt.Println()
// }
func (t *Table) addRow(Inserts [][]string, Fields []string) error {
	for i := 0; i < len(Inserts); i++ {
		err := t.addSingleRow(Inserts[i], Fields)
		if err != nil {
			return err
		}
	}
	return nil
}
func (d *Database) makeTable(q parser.Query) *Table {
	t := &Table{
		name:    q.TableName,
		rowhead: nil,
		rowtail: nil,
		index:   map[string]int{},
		columns: make([]string, len(q.Fields)),
	}
	for i := 0; i < len(q.Fields); i++ {
		t.columns[i] = q.Fields[i]
		t.index[q.Fields[i]] = i + 1
	}
	return t
}
func (d *Database) makeTableAggFunc(q parser.Query) *Table {
	t := &Table{
		name:    q.TableName,
		rowhead: nil,
		rowtail: nil,
		index:   map[string]int{},
		columns: make([]string, len(q.Fields)),
	}
	i := 0
	tempmap := make(map[string]int)

	for k := 0; k < len(q.AggregateFunc["COUNT"]); k++ {
		t.columns[i] = "COUNT" + "(" + q.AggregateFunc["COUNT"][k] + ")"
		t.index[t.columns[i]] = i + 1
		tempmap[q.AggregateFunc["COUNT"][k]]++
		i++
	}
	for k := 0; k < len(q.AggregateFunc["SUM"]); k++ {
		t.columns[i] = "SUM" + "(" + q.AggregateFunc["SUM"][k] + ")"
		t.index[t.columns[i]] = i + 1
		tempmap[q.AggregateFunc["SUM"][k]]++
		i++
	}
	for k := 0; k < len(q.AggregateFunc["AVG"]); k++ {
		t.columns[i] = "AVG" + "(" + q.AggregateFunc["AVG"][k] + ")"
		t.index[t.columns[i]] = i + 1
		tempmap[q.AggregateFunc["AVG"][k]]++
		i++
	}

	// for FUNC, FIELDS := range q.AggregateFunc {
	// 	for k := 0; k < len(FIELDS); k++ {
	// 		t.columns[i] = FUNC + "(" + FIELDS[k] + ")"
	// 		t.index[t.columns[i]] = i + 1
	// 		// fmt.Println(FUNC, FIELD)
	// 		// if tempmap[FIELD] == 0 {
	// 		// tempmap[FIELD] = 1
	// 		// } else {
	// 		tempmap[FIELDS[k]]++
	// 		// }
	// 		i++
	// 	}
	// }
	// fmt.Println(q)
	for j := 0; j < len(q.Fields); j++ {
		if tempmap[q.Fields[j]] == 0 {
			t.index[q.Fields[j]] = i + 1
			t.columns[i] = q.Fields[j]
			i++
		} else {
			tempmap[q.Fields[j]]--
		}
	}
	return t
}
func (d *Database) printTable(t *Table) {
	for i := 0; i < len(t.columns); i++ {
		fmt.Print(t.columns[i])
		for j := 0; j < 2-(len(t.columns[i])/8); j++ {
			fmt.Print("\t")
		}
	}
	fmt.Println()
	row := t.rowhead
	for row != nil {
		for i := 0; i < len(row.data); i++ {
			fmt.Print(row.data[i], "\t\t")
		}
		fmt.Println()
		row = row.next
	}
}

type Table struct {
	name string
	// schema     sql.Schema
	// partitions map[string][]sql.Row
	// keys [][]byte
	index map[string]int

	// insert int
	rowhead *Row
	rowtail *Row

	// filters    []sql.Expression
	// projection []string
	columns []string
	// lookup     sql.IndexLookup
}
