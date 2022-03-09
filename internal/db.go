package internal

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql driver
)

const (
	TYPE_VIEW  string = "view"
	TYPE_TABLE string = "table"
)

// MyDb db struct
type MyDb struct {
	Db     *sql.DB
	dbType string
	dbName string
}

// NewMyDb parse dsn
func NewMyDb(dsn string, dbType string) *MyDb {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(fmt.Sprintf("connected to db [%s] failed,err=%s", dsn, err))
	}
	return &MyDb{
		Db:     db,
		dbType: dbType,
		dbName: strings.Split(dsn, "/")[1],
	}
}

// 获取database创建sql语句
func (db *MyDb) GetDatabase() (schema string) {
	rs, err := db.Query("show create database " + db.dbName)
	if err != nil {
		log.Println(err)
		return
	}
	defer rs.Close()
	for rs.Next() {
		var vname string
		if err := rs.Scan(&vname, &schema); err != nil {
			panic(fmt.Sprintf("get table %s 's schema failed, %s", db.dbName, err))
		}
	}
	return
}

// GetTableNames table names
func (db *MyDb) GetTableNames(tType string) []string {
	rs, err := db.Query("show table status")
	if err != nil {
		panic("show tables failed:" + err.Error())
	}
	defer rs.Close()

	var tables []string
	columns, _ := rs.Columns()
	for rs.Next() {
		var values = make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rs.Scan(valuePtrs...); err != nil {
			panic("show tables failed when scan," + err.Error())
		}
		var valObj = make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			valObj[col] = v
		}
		//表table
		if tType == TYPE_TABLE && valObj["Engine"] != nil {
			tables = append(tables, valObj["Name"].(string))
		}
		//视图view
		if tType == TYPE_VIEW {
			tables = append(tables, valObj["Name"].(string))
		}
	}
	return tables
}

// GetTableSchema table schema
func (db *MyDb) GetTableSchema(name string, tType string) (schema string) {
	rs, err := db.Query(fmt.Sprintf("show create table `%s`", name))
	if err != nil {
		log.Println(err)
		return
	}
	defer rs.Close()
	for rs.Next() {
		var vname string
		//如果是表[table]
		if tType == TYPE_TABLE {
			if err := rs.Scan(&vname, &schema); err != nil {
				//如果是解析参数个数错误,则返回空
				matched, _ := regexp.MatchString("sql: expected .* destination arguments in Scan, not .*", err.Error())
				if !matched {
					panic(fmt.Sprintf("get table %s 's schema failed, %s", name, err))
				}
			}
		}
		//如果是视图[view]
		if tType == TYPE_VIEW {
			if err := rs.Scan(&vname, &schema, &vname, &vname); err != nil {
				//如果是解析参数个数错误,则返回空
				matched, _ := regexp.MatchString("sql: expected .* destination arguments in Scan, not .*", err.Error())
				if !matched {
					panic(fmt.Sprintf("get table %s 's schema failed, %s", name, err))
				}
			}
		}
	}
	return
}

// Query execute sql query
func (db *MyDb) Query(query string, args ...interface{}) (*sql.Rows, error) {
	//log.Println("[SQL]", "["+db.dbType+"]", query, args)
	return db.Db.Query(query, args...)
}
