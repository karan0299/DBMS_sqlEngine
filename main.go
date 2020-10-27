package main

import (
	"fmt"
	"sqlengine/parser"
)

func main() {
	st := "SELECT COUNT(p) , q FROM t WHERE p = q AND p != 'q' GROUP BY q , p HAVING COUNT(p) > 1 AND AVG(q) < 3"
	q, err := parser.Parse(st)
	if err == nil {
		fmt.Println(q)
	}
}
