package main

import "gopkg.in/src-d/go-errors.v1"

var (
	// ErrInvalidType is thrown when there is an unexpected type at some part of
	// the execution tree.
	ErrInvalidType     = errors.NewKind("invalid type: %s")
	ErrUnknownOperator = errors.NewKind("Unknown Operator: %s")

	// ErrTableAlreadyExists is thrown when someone tries to create a
	// table with a name of an existing one
	ErrTableAlreadyExists = errors.NewKind("table with name %s already exists")

	// ErrTableNotFound is returned when the table is not available from the
	// current scope.
	ErrTableNotFound = errors.NewKind("table not found: %s")

	//ErrUnexpectedRowLength is thrown when the obtained row has more columns than the schema
	ErrUnexpectedRowLength = errors.NewKind("expected %d values, got %d")

	// ErrInvalidChildrenNumber is returned when the WithChildren method of a
	// node or expression is called with an invalid number of arguments.
	ErrInvalidChildrenNumber = errors.NewKind("%T: invalid children number, got %d, expected %d")

	// ErrDeleteRowNotFound
	ErrDeleteRowNotFound = errors.NewKind("row was not found when attempting to delete").New()
)
