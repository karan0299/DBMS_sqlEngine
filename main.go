package main

import (
	"bufio"
	"os"
	"sqlengine/parser"
)

func main() {
<<<<<<< HEAD
	st := "SELECT COUNT(p) , q FROM t WHERE p = q AND p != 'q' GROUP BY q , p HAVING COUNT(p) > 1 AND AVG(q) < 3"
=======
	st := "SELECT COUNT(p) , q,r FROM table1 WHERE p = q AND p != 'q' GROUP BY q , p"
	scanner := bufio.NewScanner(os.Stdin)
>>>>>>> Adding code for Database and tables along with Query support for queries 1,2&8
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
