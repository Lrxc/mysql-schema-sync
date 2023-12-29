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

		rows2, err := sc.SourceDb.Query("select * from " + table)
		if err != nil {
			log.Println(err)
		}

		//所有索引
		index, err := sc.DestDb.Query("show index from " + table)
		if err != nil {
			log.Fatal(err)
		}

		if rows1 != nil && rows2 != nil {
			indexRes := getResult(index)
			result1 := getResult(rows1)
			result2 := getResult(rows2)

			//获取主键字段名
			primaryKey := primaryKey(indexRes)

			//计算哪个记录更多
			count := count(len(result1), len(result2))

			//每一条记录
			for i := 0; i < count; i++ {
				//fmt.Printf("对比表数据: %s-%d \n", table, i)

				//判断是否还有数据
				var d1, d2 map[string]string
				if len(result1) > i {
					d1 = result1[i]
				}
				if len(result2) > i {
					d2 = result2[i]
				}

				if primaryKey != "" {
					compareDiffByPrimary(table, primaryKey, d1, d2)
				} else {
					compareDiffNoPrimary(table, d1, d2)
				}
			}
		}
	}
}

// 获取主键字段名
func primaryKey(indexRes []map[string]string) string {
	//判断是否存在主键
	for _, value := range indexRes {
		key, exist := value["Key_name"]

		if exist && strings.EqualFold(key, "PRIMARY") {
			return value["Column_name"]
		}
	}
	return ""
}

func count(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// 无主键
func compareDiffNoPrimary(table string, d1, d2 map[string]string) {
	//alter
	if d1 != nil && d2 != nil {
		//先删除
		var columns, values []string
		for k, v := range d1 {
			columns = append(columns, k+"="+v)
		}
		fmt.Printf("DELETE FROM %s WHERE %s;\n", table, strings.Join(columns, " and "))

		//再插入新的
		for k, v := range d2 {
			columns = append(columns, k)
			values = append(values, v)
		}

		fmt.Printf("INSERT INTO %s(%s) VALUES (%s);\n", table, strings.Join(columns, ","), strings.Join(values, ","))
	}

	//delete
	if d1 != nil && d2 == nil {
		var row []string
		for k, v := range d1 {
			row = append(row, k+"="+v)
		}
		fmt.Printf("DELETE FROM %s WHERE %s;\n", table, strings.Join(row, " and "))
	}

	//insert
	if d1 == nil && d2 != nil {
		var row []string
		for _, v := range d2 {
			row = append(row, v)
		}
		fmt.Printf("INSERT INTO %s VALUES (%s);\n", table, strings.Join(row, ","))
	}
}

// 有主键
func compareDiffByPrimary(table, primaryKey string, d1, d2 map[string]string) {
	//alter
	if d1 != nil && d2 != nil {
		id := d1[primaryKey]

		//先删除
		fmt.Printf("DELETE FROM %s WHERE id = '%s';\n", table, id)

		var columns, values []string
		for k, v := range d2 {
			columns = append(columns, k)
			values = append(values, v)
		}
		fmt.Printf("INSERT INTO %s(%s) VALUES (%s);\n", table, strings.Join(columns, ","), strings.Join(values, ","))
	}

	//delete
	if d1 != nil && d2 == nil {
		id := d1[primaryKey]
		fmt.Printf("DELETE FROM %s WHERE id = '%s';\n", table, id)
	}

	//insert
	if d1 == nil && d2 != nil {
		var row []string
		for _, v := range d2 {
			row = append(row, v)
		}
		fmt.Printf("INSERT INTO %s VALUES (%s);\n", table, strings.Join(row, ","))
	}
}

// 遍历结果
func getResult(rows *sql.Rows) []map[string]string {
	columns, _ := rows.Columns()

	// 读取并比较数据
	var data []map[string]string
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

		res := make(map[string]string)
		for i, v := range values {
			key := columns[i]
			res[key] = fmt.Sprintf("%s", v)
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
