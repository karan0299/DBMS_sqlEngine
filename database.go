package main

import (
	"sqlengine/parser"
)

// Database is an in-memory database.
type Database struct {
	name   string
	tables map[string]*Table
}

// NewDatabase creates a new database with the given name.
func NewDatabase(name string) *Database {
	return &Database{
		name:   name,
		tables: map[string]*Table{},
	}
}

// Name returns the database name.
func (d *Database) Name() string {
	return d.name
}

// Tables returns all tables in the database.
func (d *Database) Tables() map[string]*Table {
	return d.tables
}

// AddTable adds a new table to the database.
func (d *Database) AddTable(q parser.Query) {
	d.tables[q.TableName] = d.makeTable(q)
}

// DropTable drops the table with the given name
func (d *Database) DropTable(name string) error {
	_, ok := d.tables[name]
	if !ok {
		return ErrTableNotFound.New(name)
	}

	delete(d.tables, name)
	return nil
}
