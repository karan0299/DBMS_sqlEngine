package main

import (
	"fmt"
	"sqlengine/parser"
)

func main() {
	st := "SELECT COUNT(p) , q FROM t WHERE p = q AND p != 'q' GROUP BY q , p"
	q, err := parser.Parse(st)
	if err == nil {
		fmt.Println(q)
	}
}
