package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sqlengine/parser"

	"github.com/gin-gonic/gin"
)

type data struct {
	ID   int      `json:"ID"`
	Sets []result `json:"sets"`
}

type result struct {
	Results          rowColResult           `json:"RESULTS"`
	Succeeded        bool                   `json:"SUCCEEDED"`
	Statement        string                 `json:"STATEMENT"`
	ErrorMessage     string                 `json:"ERRORMESSAGE,omitempty"`
	ExecutionTime    int                    `json:"EXECUTIONTIME"`
	Executionplanraw map[string]interface{} `json:"EXECUTIONPLANRAW"`
	Executionplan    map[string]interface{} `json:"EXECUTIONPLAN"`
}

type schema struct {
	ID              string `json:"_id"`
	Short           string `json:"short_code"`
	SchemaStructure []ss   `json:"schema_structure"`
}

type ss struct {
	TableName string `json:"table_name"`
	TableType string `json:"table_type"`
	Columns   []col  `json:"columns"`
}

type col struct {
	Name    string `json:"name"`
	TypeCol string `json:"type"`
}

type dbtypes struct {
	Result []dbmodel `json:"result"`
}

type dbmodel struct {
	Db_type_id      int    `json:"db_type_id"`
	Context         string `json:"context"`
	Full_name       string `json:"full_name"`
	Simple_name     string `json:"simple_name"`
	Sample_fragment string `json:"sample_fragment"`
	Batch_separator string `json:"batch_separator"`
	Classname       string `json:"classname"`
	NumHosts        int    `json:"num_hosts"`
}

type rowColResult struct {
	Cols []string   `json:"COLUMNS"`
	Data [][]string `json:"DATA"`
}

type RequestData struct {
	Sql string `json:"sql"`
}

func main() {
	database := &Database{}
	// st1 := "DROP TABLE t"
	// scanner := bufio.NewScanner(os.Stdin)
	// // q1, err := parser.Parse(st1)
	// // if err == nil {
	// // 	fmt.Println(q1)
	// // }
	// // database := "database1"
	// database, _ := getDatabase("DOG")
	// fmt.Println(database.tables)
	// //database.AddTable(q)
	// // fmt.Println(database)
	// for scanner.Scan() {
	// 	st := scanner.Text()
	// 	// fmt.Println()
	// 	if st == "exit" {
	// 		break
	// 	}
	// 	q, err := parser.Parse(st)
	// 	// fmt.Println(q)
	// 	if err == nil {
	// 		// fmt.Println(q)
	// 		database.executeQuery(q)
	// 	}
	// }
<<<<<<< HEAD
	// database := "database1"
	database := getDatabase("database1")
	fmt.Println(database.tables)
	//database.AddTable(q)
	// fmt.Println(database)
	for scanner.Scan() {
		st = scanner.Text()
		fmt.Println()
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
=======
>>>>>>> COMPLETE SERVER OPERATION EXPECT SENDING DATA IN ROWS AND COLS

	// // stores the given database
	// store(database)
	// st := "CREATE TABLE doc ( name int(3), roll varchar(5) )"
	// q, err := parser.SchemaParse(st)
	// if err == nil {
	// 	fmt.Println(q)
	// }
	router := gin.Default()

	router.POST("/parse", func(c *gin.Context) {
		// s := `{
		// 	"ID" : 4875,
		// 	"sets" : [ {
		// 	  "RESULTS" : {
		// 		"COLUMNS" : [ "id", "rev", "content" ],
		// 		"DATA" : [ [ 1, 3, "The earth is like a ball." ], [ 2, 1, "One hundred angels can dance on the head of a pin" ] ]
		// 	  },
		// 	  "SUCCEEDED" : true,
		// 	  "STATEMENT" : "-- based on answer https://stackoverflow.com/a/7745635/808921\n\nSELECT a.id, a.rev, a.content\nFROM 'docs' a\nINNER JOIN (\n    SELECT id, MAX(rev) rev\n    FROM 'docs'\n    GROUP BY id\n) b ON a.id = b.id AND a.rev = b.rev",
		// 	  "EXECUTIONTIME" : 17,
		// 	  "EXECUTIONPLANRAW" : {
		// 		"COLUMNS" : [ "id", "select_type", "table", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra" ],
		// 		"DATA" : [ [ "1", "PRIMARY", "a", "ALL", "PRIMARY", null, null, null, "4", "100.00", null ], [ "1", "PRIMARY", "<derived2>", "ref", "<auto_key0>", "<auto_key0>", "9", "db_9_a6c585.a.id,db_9_a6c585.a.rev", "2", "100.00", "Using index" ], [ "2", "DERIVED", "docs", "index", "PRIMARY", "PRIMARY", "8", null, "4", "75.00", "Using index" ] ]
		// 	  },
		// 	  "EXECUTIONPLAN" : {
		// 		"COLUMNS" : [ "id", "select_type", "table", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra" ],
		// 		"DATA" : [ [ "1", "PRIMARY", "a", "ALL", "PRIMARY", null, null, null, "4", "100.00", null ], [ "1", "PRIMARY", "<derived2>", "ref", "<auto_key0>", "<auto_key0>", "9", "db_9_a6c585.a.id,db_9_a6c585.a.rev", "2", "100.00", "Using index" ], [ "2", "DERIVED", "docs", "index", "PRIMARY", "PRIMARY", "8", null, "4", "75.00", "Using index" ] ]
		// 	  }
		// 	} ]
		//   }`
		data := &data{
			Sets: []result{},
		}

		res := &result{}
		x, _ := ioutil.ReadAll(c.Request.Body)

		req := &RequestData{}
		err := json.Unmarshal([]byte(x), &req)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{})
		}

		res.ExecutionTime = 1
		res.Executionplan = nil
		res.Executionplanraw = nil

		res.Statement = req.Sql

		q, err := parser.Parse(req.Sql)

		if err != nil {
			res.Succeeded = false
			res.ErrorMessage = err.Error()
			data.Sets = append(data.Sets, *res)

			c.JSON(http.StatusOK, gin.H{
				"ID":   123,
				"sets": data.Sets,
			})
			return
		}

		if q.Type == 5 {
			err = database.DropTable(q.TableName)
			rowcol := rowColResult{}
			if err != nil {
				rowcol.Cols = append(rowcol.Cols, "Drop Table : Failure due to error")
				rowcol.Data = append(rowcol.Data, []string{err.Error()})
			} else {
				rowcol.Cols = append(rowcol.Cols, "Drop Table")
				rowcol.Data = append(rowcol.Data, []string{"Success"})
			}
			res.Results = rowcol
			res.Succeeded = true
			data.Sets = append(data.Sets, *res)
			fmt.Println(data.Sets)
			c.JSON(http.StatusOK, gin.H{
				"ID":   123,
				"sets": data.Sets,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"ID":   123,
			"sets": data.Sets,
		})
		return
	})

	router.POST("/build_schema", func(c *gin.Context) {

		sch := []ss{}
		data := &schema{
			ID:              "9_a6c585",
			Short:           "a6c585",
			SchemaStructure: sch,
		}

		x, _ := ioutil.ReadAll(c.Request.Body)

		req := &RequestData{}
		err := json.Unmarshal([]byte(x), &req)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{})
			return
		}

		res, err := parser.SchemaParse(req.Sql)

		if err != nil {
			c.JSON(http.StatusOK, gin.H{
				"error": err.Error(),
			})
			return
		}

		if res.TableOrDB == "DB" {
			db := NewDatabase(res.Name)
			err = store(db)
			if err != nil {
				c.JSON(http.StatusOK, gin.H{
					"type": "createdb",
					"msg":  "Failed to Create database",
				})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"type": "createdb",
				"msg":  "",
			})
			return
		}

		if res.Use == true {
			db, err := getDatabase(res.Name)
			fmt.Println(err)
			if err != nil {
				c.JSON(http.StatusOK, gin.H{
					"type": "usedb",
					"msg":  "Failed to use db , either it doesnt exist or some technical error ",
				})
				return
			}
			database = db
			c.JSON(http.StatusOK, gin.H{
				"type": "usedb",
				"msg":  "",
			})
			return
		}
		fmt.Println(database.name)

		mapq := mapSchemaToQuery(&res)

		database.AddTable(*mapq)

		schemaStr := &ss{
			TableName: res.Name,
			TableType: "TABLE",
		}

		for key, val := range res.Columns {
			column := col{
				Name:    key,
				TypeCol: fmt.Sprintf("%s(%v)", val.Dtype, val.Size),
			}
			schemaStr.Columns = append(schemaStr.Columns, column)
		}

		data.SchemaStructure = append(data.SchemaStructure, *schemaStr)
		c.JSON(http.StatusOK, data)

		return

	})

	router.GET("/dbTypes", func(c *gin.Context) {
		s := `{  "result" : [ {
			"db_type_id" : 9,
			"context" : "host",
			"full_name" : "MySQL 5.6",
			"simple_name" : "MySQL",
			"sample_fragment" : "9/a6c585/1",
			"batch_separator" : null,
			"classname" : "org.gjt.mm.mysql.Driver",
			"num_hosts" : 1
		  }]}`
		data := &dbtypes{
			Result: []dbmodel{},
		}
		err := json.Unmarshal([]byte(s), data)
		fmt.Println(data)
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{
			"result": data.Result,
		})
	})

	router.Run(":1234")
}

func mapSchemaToQuery(sc *parser.Schema) *parser.Query {
	q := &parser.Query{
		TableName: sc.Name,
	}

	for key := range sc.Columns {
		q.Fields = append(q.Fields, key)
	}

	return q
}
