package main

import (
	"bufio"
	"os"
	"sqlengine/parser"
)

func main() {
	st := "SELECT COUNT(p) , q,r FROM table1 WHERE p = q AND p != 'q' GROUP BY q , p"
	scanner := bufio.NewScanner(os.Stdin)
	q, err := parser.Parse(st)
	// if err == nil {
	// 	fmt.Println(q)
	// }
	// database := "database1"
	database := NewDatabase("database1")
	database.AddTable(q)
	// fmt.Println(database)
	for scanner.Scan() {
		st = scanner.Text()
		// fmt.Println()
		if st == "exit" {
			break
		}
		q, err = parser.Parse(st)
		// fmt.Println(q)
		if err == nil {
			// fmt.Println(q)
			database.executeQuery(q)
		}
		// fmt.Println()
	}
}
