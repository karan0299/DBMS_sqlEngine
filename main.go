package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sqlengine/parser"

	"github.com/gin-gonic/gin"
)

type data struct {
	ID   int      `json:"ID"`
	Sets []result `json:"sets"`
}

type result struct {
	Results          map[string]interface{} `json:"RESULTS"`
	Succeeded        bool                   `json:"SUCCEEDED"`
	Statement        string                 `json:"STATEMENT"`
	ExecutionTIme    int                    `json:"EXECUTIONTIME"`
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

	//stores the given database
	store(database)
	st := "UPDATE table_name SET column1 = 'value1' , column2 = 'value2' WHERE condition = p"
	q, err := parser.Parse(st)
	if err == nil {
		fmt.Println(q)
	}
	router := gin.Default()

	router.GET("/parse", func(c *gin.Context) {
		s := `{
			"ID" : 4875,
			"sets" : [ {
			  "RESULTS" : {
				"COLUMNS" : [ "id", "rev", "content" ],
				"DATA" : [ [ 1, 3, "The earth is like a ball." ], [ 2, 1, "One hundred angels can dance on the head of a pin" ] ]
			  },
			  "SUCCEEDED" : true,
			  "STATEMENT" : "-- based on answer https://stackoverflow.com/a/7745635/808921\n\nSELECT a.id, a.rev, a.content\nFROM 'docs' a\nINNER JOIN (\n    SELECT id, MAX(rev) rev\n    FROM 'docs'\n    GROUP BY id\n) b ON a.id = b.id AND a.rev = b.rev",
			  "EXECUTIONTIME" : 17,
			  "EXECUTIONPLANRAW" : {
				"COLUMNS" : [ "id", "select_type", "table", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra" ],
				"DATA" : [ [ "1", "PRIMARY", "a", "ALL", "PRIMARY", null, null, null, "4", "100.00", null ], [ "1", "PRIMARY", "<derived2>", "ref", "<auto_key0>", "<auto_key0>", "9", "db_9_a6c585.a.id,db_9_a6c585.a.rev", "2", "100.00", "Using index" ], [ "2", "DERIVED", "docs", "index", "PRIMARY", "PRIMARY", "8", null, "4", "75.00", "Using index" ] ]
			  },
			  "EXECUTIONPLAN" : {
				"COLUMNS" : [ "id", "select_type", "table", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra" ],
				"DATA" : [ [ "1", "PRIMARY", "a", "ALL", "PRIMARY", null, null, null, "4", "100.00", null ], [ "1", "PRIMARY", "<derived2>", "ref", "<auto_key0>", "<auto_key0>", "9", "db_9_a6c585.a.id,db_9_a6c585.a.rev", "2", "100.00", "Using index" ], [ "2", "DERIVED", "docs", "index", "PRIMARY", "PRIMARY", "8", null, "4", "75.00", "Using index" ] ]
			  }
			} ]
		  }`
		data := &data{
			Sets: []result{},
		}
		err := json.Unmarshal([]byte(s), data)
		fmt.Println(data)
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{
			"ID":   123,
			"sets": data.Sets,
		})
	})

	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"data": "heLOO",
		})
	})

	router.GET("/build_schema", func(c *gin.Context) {

		sch := []ss{}
		data := &schema{
			SchemaStructure: sch,
		}
		s := `{
			"_id" : "9_a6c585",
			"short_code" : "a6c585",
			"schema_structure" : [ {
			  "table_name" : "docs",
			  "table_type" : "TABLE",
			  "columns" : [ {
				"name" : "id",
				"type" : "INT UNSIGNED(10)"
			  }, {
				"name" : "rev",
				"type" : "INT UNSIGNED(10)"
			  }, {
				"name" : "content",
				"type" : "VARCHAR(200)"
			  } ]
			} ]
		  }`
		err := json.Unmarshal([]byte(s), data)
		fmt.Println(data)
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{
			"_id":              data.ID,
			"short_name":       data.Short,
			"schema_structure": data.SchemaStructure,
		})

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
