package main

import (
	"bufio"
	"fmt"
	"os"
	"sqlengine/parser"
)

func main() {
	//st := "SELECT field1, field2, field3 FROM table1"
	scanner := bufio.NewScanner(os.Stdin)
	//q, err := parser.Parse(st)
	// if err == nil {
	//fmt.Println(q)
	// }
	// database := "database1"
	database := getDatabase("database1")
	fmt.Println(database.tables)
	//database.AddTable(q)
	// fmt.Println(database)
	for scanner.Scan() {
		st := scanner.Text()
		// fmt.Println()
		if st == "exit" {
			break
		}
		q, err := parser.Parse(st)
		// fmt.Println(q)
		if err == nil {
			// fmt.Println(q)
			database.executeQuery(q)
		}
	}

	//stores the given database
	store(database)
}
