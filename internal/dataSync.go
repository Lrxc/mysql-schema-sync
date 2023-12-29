package internal

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// 差异数据
func CheckDataDiff(scs *statics, cfg *Config, sc *SchemaSync, tType string) {
	newTables := sc.GetTableNames(tType)

	for _, table := range newTables {
		fmt.Println(table)

		rows1, err := sc.DestDb.Query("select * from " + table)
		if err != nil {
			log.Fatal(err)
		}

		index, err := sc.DestDb.Query("show index from " + table)
		if err != nil {
			log.Fatal(err)
		}

		rows2, err := sc.SourceDb.Query("select * from " + table)
		if err != nil {
			log.Println(err)
		}

		if rows1 != nil && rows2 != nil {
			indexRes := getResult(index)
			result1 := getResult(rows1)
			result2 := getResult(rows2)

			for i := range indexRes {
				fmt.Println(indexRes[i])
			}

			for i := range result1 {
				fmt.Println(result1[i])

				if len(result2)-1 >= i {
					fmt.Println(result2[i])
				}

				row := result1[i]
				row2 := result2[i]

				// 比较两个行
				if strings.Join(row, ",") != strings.Join(row2, ",") {
					// 生成差异 SQL
					switch {
					case row[0] != row2[0]:
						// 插入
						fmt.Println("INSERT INTO table VALUES (%s);\n", strings.Join(row, ","))
					case row[0] == row2[0]:
						// 更新
						columns, _ := rows1.Columns()
						for i := range columns {
							if row[i] != row2[i] {
								fmt.Println("UPDATE table SET %s = '%s' WHERE id = '%s';\n", 2, row[i], row[0])
								break
							}
						}
					case row[0] == "" && row2[0] != "":
						// 删除
						fmt.Println("DELETE FROM table WHERE id = '%s';\n", row2[0])
					}
				}
			}
		}
	}
}

// 遍历结果
func getResult(rows *sql.Rows) [][]string {
	columns, _ := rows.Columns()

	// 读取并比较数据
	var data [][]string
	for rows.Next() {
		// 声明一个切片用于存储每一行的数据
		values := make([]interface{}, len(columns))
		scans := make([]interface{}, len(columns))
		for i := range values {
			scans[i] = &values[i]
		}

		err := rows.Scan(scans...)
		if err != nil {
			log.Fatal(err)
		}

		var res []string = make([]string, len(columns))
		for i := range values {
			//interface 转string
			res[i] = fmt.Sprintf("%s", values[i])
		}
		data = append(data, res)
	}
	return data
}

func getAllTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}

func getTableColumns(db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.Query("SHOW COLUMNS FROM " + tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName, columnType, isNull, key, extra string
		err := rows.Scan(&columnName, &columnType, &isNull, &key, &extra)
		if err != nil {
			return nil, err
		}
		columns = append(columns, columnName)
	}
	return columns, nil
}

func equal(row1, row2 []string) bool {
	// 检查两行数据是否相等
	// 一种简单的比较方式是将两行数据转换为字符串并进行比较
	return strings.Join(row1, "|") == strings.Join(row2, "|")
}

func generateInsertSQL(tableName string, values []string, columns []string) string {
	insertValues := "'" + strings.Join(values, "','") + "'"
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, strings.Join(columns, ","), insertValues)
}

func generateDeleteSQL(tableName string, values []string, columns []string) string {
	conditions := make([]string, len(columns))
	for i, col := range columns {
		conditions[i] = fmt.Sprintf("%s='%s'", col, values[i])
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s;", tableName, strings.Join(conditions, " AND "))
}
