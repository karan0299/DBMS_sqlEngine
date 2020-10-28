package main

import (
	"fmt"
	"sqlengine/parser"
)

type Row struct {
	data []string
	next *Row
}

func (t *Table) addSingleRow(data []string, Fields []string) {
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
		t.rowtail.data[t.index[Fields[i]]-1] = data[i]
	}

}

func (t *Table) addRow(Inserts [][]string, Fields []string) {
	for i := 0; i < len(Inserts); i++ {
		t.addSingleRow(Inserts[i], Fields)
	}
}
func (d *Database) makeTable(q parser.Query) *Table {
	t := &Table{
		name:    q.TableName,
		rowhead: nil,
		rowtail: nil,
		index:   map[string]int{},
		columns: q.Fields,
	}
	for i := 0; i < len(q.Fields); i++ {
		t.index[q.Fields[i]] = i + 1
	}
	return t
}
func (d *Database) printTable(t *Table) {
	for i := 0; i < len(t.columns); i++ {
		fmt.Print(t.columns[i], "\t\t")
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
