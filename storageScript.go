package main

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sqlengine/parser"
)

// exists returns whether the given file or directory exists
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func getDatabase(name string) (*Database, error) {
	e, err := exists("./Databases/" + name)
	if err == nil {
		if !e {
			fmt.Println("doesnt exist")
			err := fmt.Errorf("Database does not exist")
			return NewDatabase(name), err
		}
		files, err := ioutil.ReadDir("./Databases/" + name)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		database := NewDatabase(name)
		for _, f := range files {
			csvFile, err := os.Open("./Databases/" + name + "/" + f.Name())
			if err != nil {
				fmt.Println(err)
				return database, err
			}
			fmt.Println("Successfully Opened CSV file")
			defer csvFile.Close()

			csvLines, err := csv.NewReader(csvFile).ReadAll()
			if err != nil {
				fmt.Println(err)
				return database, err
			}
			var cnt int = 0
			fields := []string{}
			var filename = f.Name()
			var extension = filepath.Ext(filename)
			var name = filename[0 : len(filename)-len(extension)]
			for _, line := range csvLines {
				if cnt == 0 {
					fields = line
					st := "SELECT "
					for val, x := range line {
						if val != len(line)-1 {
							st = st + x + ", "
						} else {
							st = st + x + " "
						}
					}
					st = st + "FROM " + name
					q, err := parser.Parse(st)
					if err == nil {
						database.AddTable(q)
					} else {
						fmt.Println(err)
						return database, err
					}
					cnt = cnt + 1
				} else {
					database.tables[name].addSingleRow(line, fields)
				}
			}

		}
		return database, nil

	}
	fmt.Println(err)
	return nil, err

}

func store(database *Database) error {

	e, err := exists("./Databases/" + database.name)
	if err == nil {
		if !e {
			os.Mkdir("./Databases/"+database.name, 0775)
		}
	} else {
		fmt.Println(err)
		return err
	}
	for tableName, tableAddress := range database.tables {
		e, err = exists("./Databases/" + database.name + "/" + tableName)
		if err == nil {
			csvFile, err := os.Create("./Databases/" + database.name + "/" + tableName + ".csv")
			if err != nil {
				log.Fatalf("failed creating file: %s", err)
				return err
			}

			csvwriter := csv.NewWriter(csvFile)
			empData := [][]string{}
			empData = append(empData, tableAddress.columns)
			var currRow *Row
			currRow = tableAddress.rowhead
			for currRow != nil {
				empData = append(empData, currRow.data)
				currRow = currRow.next
			}

			for _, empRow := range empData {
				_ = csvwriter.Write(empRow)
			}
			csvwriter.Flush()
			csvFile.Close()
		} else {
			fmt.Println(err)
			return err
		}
	}
	return nil
}
