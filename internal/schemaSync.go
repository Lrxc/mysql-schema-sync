package internal

import (
	"fmt"
	fmtm "github.com/hidu/mysql-schema-sync/internal/logm/fmtm"
	logm "github.com/hidu/mysql-schema-sync/internal/logm/logm"
	"log"
	"regexp"
	"strings"
)

// SchemaSync 配置文件
type SchemaSync struct {
	Config   *Config
	SourceDb *MyDb
	DestDb   *MyDb
}

// NewSchemaSync 对一个配置进行同步
func NewSchemaSync(config *Config) *SchemaSync {
	s := new(SchemaSync)
	s.Config = config
	s.SourceDb = NewMyDb(config.SourceDSN, "source")
	s.DestDb = NewMyDb(config.DestDSN, "dest")
	return s
}

// GetNewTableNames 获取所有新增加的表名
func (sc *SchemaSync) GetNewTableNames() []string {
	sourceTables := sc.SourceDb.GetTableNames(TYPE_TABLE)
	destTables := sc.DestDb.GetTableNames(TYPE_TABLE)

	var newTables []string

	for _, name := range sourceTables {
		if !inStringSlice(name, destTables) {
			newTables = append(newTables, name)
		}
	}
	return newTables
}

// 数据库db是否存在,新增或删除
func (sc *SchemaSync) GetDatabase(config *Config) {
	//获取建db库sql语句
	sourceSql := sc.SourceDb.GetDatabase()
	descSql := sc.DestDb.GetDatabase()

	//如果db库已经被删除
	if len(sourceSql) == 0 && len(descSql) != 0 {
		sql := "DROP DATABASE IF EXISTS " + sc.DestDb.dbName
		fmtm.Println("-- 删除 Database")
		fmtm.Println(sql + ";")
		//执行删除db语句
		sc.SyncSQL4Dest(sql, nil)
		//手动抛出异常,结束流程
		panic("drop database " + sc.DestDb.dbName + " ok")
	}
	//如果db库是新增
	if len(sourceSql) != 0 && len(descSql) == 0 {
		//重新连接 mysql 系统数据库,用于创建新db库
		sc.DestDb = NewMyDb(strings.Replace(config.DestDSN, sc.DestDb.dbName, "mysql", -1), "dest")

		sourceSql = strings.Replace(sourceSql, "CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS", -1)
		fmtm.Println("-- 创建 Database")
		fmtm.Println(sourceSql + ";")
		//执行创建db语句
		err := sc.SyncSQL4Dest(sourceSql, nil)
		if err != nil {
			panic("create database " + sc.DestDb.dbName + " error")
		}
		//创建成功后,改回默认数据库
		sc.DestDb = NewMyDb(config.DestDSN, "dest")
	}
}

// 合并源数据库和目标数据库的表名
func (sc *SchemaSync) GetTableNames(tType string) []string {
	sourceTables := sc.SourceDb.GetTableNames(tType)
	destTables := sc.DestDb.GetTableNames(tType)
	var tables []string
	tables = append(tables, destTables...)
	for _, name := range sourceTables {
		if !inStringSlice(name, tables) {
			tables = append(tables, name)
		}
	}
	return tables
}

// 删除表创建引擎信息，编码信息，分区信息，已修复同步表结构遇到分区表异常退出问题，对于分区表，只会同步字段，索引，主键，外键的变更
func RemoveTableSchemaConfig(schema string) string {
	return strings.Split(schema, "ENGINE")[0]
}

func (sc *SchemaSync) getAlterDataByTable(table string, cfg *Config, tType string) *TableAlterData {
	alter := new(TableAlterData)
	alter.Table = table
	alter.Type = alterTypeNo

	sSchema := sc.SourceDb.GetTableSchema(table, tType)
	dSchema := sc.DestDb.GetTableSchema(table, tType)
	alter.SchemaDiff = newSchemaDiff(table, RemoveTableSchemaConfig(sSchema), RemoveTableSchemaConfig(dSchema))

	if sSchema == dSchema {
		return alter
	}
	if sSchema == "" {
		alter.Type = alterTypeDropTable
		alter.Comment = "源数据库不存在，删除目标数据库多余的表"
		alter.SQL = append(alter.SQL, fmt.Sprintf("drop table `%s`;", table))
		return alter
	}
	if dSchema == "" {
		alter.Type = alterTypeCreate
		alter.Comment = "目标数据库不存在，创建"
		//先删除同名的表或视图
		if tType == TYPE_TABLE {
			alter.SQL = append(alter.SQL, fmt.Sprintf("DROP VIEW IF EXISTS `%s`;", table))
		} else {
			alter.SQL = append(alter.SQL, fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", table))
		}
		alter.SQL = append(alter.SQL, sSchema+";")
		return alter
	}

	diff := sc.getSchemaDiff(alter)
	if len(diff) == 0 {
		return alter
	}

	alter.Type = alterTypeAlter
	alter.Comment = "表结构修改"
	if cfg.SingleSchemaChange {
		for _, diffSql := range diff {
			upsql := addUpdate(sc.DestDb.dbName, table, diffSql)
			if upsql != "" {
				alter.SQL = append(alter.SQL, upsql)
			}
			alter.SQL = append(alter.SQL, fmt.Sprintf("ALTER TABLE `%s`\n%s;", table, diffSql))
		}
	} else {
		upsql := addUpdate(sc.DestDb.dbName, table, strings.Join(diff, ",\n"))
		if upsql != "" {
			alter.SQL = append(alter.SQL, upsql)
		}
		alter.SQL = append(alter.SQL, fmt.Sprintf("ALTER TABLE `%s`\n%s;", table, strings.Join(diff, ",\n")))
	}
	return alter
}

//针对 NOT NULL DEFAULT 情况,需要先update数据才行
func addUpdate(dbName string, table string, sql string) (upSql string) {
	//是否包含特定内容
	matched, _ := regexp.MatchString("CHANGE .* NOT NULL DEFAULT .*", sql)
	if matched {
		//获取字段名
		key := regexp.MustCompile("`.*?`").FindString(sql)
		//update语句
		upSql = fmt.Sprintf("UPDATE `%s`\nSET %s = 0 WHERE %s IS NULL;", table, key, key)
	}
	return
}

func (sc *SchemaSync) getSchemaDiff(alter *TableAlterData) []string {
	sourceMyS := alter.SchemaDiff.Source
	destMyS := alter.SchemaDiff.Dest
	table := alter.Table
	var beforeFieldName string = ""
	var alterLines []string
	var fieldCount int = 0
	// 比对字段
	for el := sourceMyS.Fields.Front(); el != nil; el = el.Next() {
		if sc.Config.IsIgnoreField(table, el.Key.(string)) {
			log.Printf("ignore column %s.%s", table, el.Key.(string))
			continue
		}
		var alterSQL string
		if destDt, has := destMyS.Fields.Get(el.Key); has {
			if el.Value != destDt {
				alterSQL = fmt.Sprintf("CHANGE `%s` %s", el.Key, el.Value)
			}
			beforeFieldName = el.Key.(string)
		} else {
			if len(beforeFieldName) == 0 {
				if fieldCount == 0 {
					alterSQL = "ADD " + el.Value.(string) + " FIRST"
				} else {
					alterSQL = "ADD " + el.Value.(string)
				}

			} else {
				alterSQL = "ADD " + el.Value.(string) + " AFTER " + beforeFieldName
			}
			beforeFieldName = el.Key.(string)
		}

		if alterSQL != "" {
			logm.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, el.Key.(string)), "alterSQL=", alterSQL)
			alterLines = append(alterLines, alterSQL)
		} else {
			log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, el.Key.(string)), "not change")
		}
		fieldCount++
	}

	// 源库已经删除的字段
	if sc.Config.Drop {
		for _, name := range destMyS.Fields.Keys() {
			if sc.Config.IsIgnoreField(table, name.(string)) {
				log.Printf("ignore column %s.%s", table, name)
				continue
			}
			if _, has := sourceMyS.Fields.Get(name); !has {
				alterSQL := fmt.Sprintf("drop `%s`", name)
				alterLines = append(alterLines, alterSQL)
				logm.Println("[Debug] check column.drop ", fmt.Sprintf("%s.%s", table, name), "alterSQL=", alterSQL)
			} else {
				log.Println("[Debug] check column.drop ", fmt.Sprintf("%s.%s", table, name), "not change")
			}
		}
	}

	// 多余的字段暂不删除

	// 比对索引
	for indexName, idx := range sourceMyS.IndexAll {
		if sc.Config.IsIgnoreIndex(table, indexName) {
			log.Printf("ignore index %s.%s", table, indexName)
			continue
		}
		dIdx, has := destMyS.IndexAll[indexName]
		//logm.Println("[Debug] indexName---->[", fmt.Sprintf("%s.%s", table, indexName), "] dest_has:", has, "\ndest_idx:", dIdx, "\nsource_idx:", idx)
		var alterSQLs []string
		if has {
			if idx.SQL != dIdx.SQL {
				alterSQLs = append(alterSQLs, idx.alterAddSQL(true)...)
			}
		} else {
			alterSQLs = append(alterSQLs, idx.alterAddSQL(false)...)
		}
		if len(alterSQLs) > 0 {
			alterLines = append(alterLines, alterSQLs...)
			logm.Println("[Debug] check index.alter ", fmt.Sprintf("%s.%s", table, indexName), "alterSQL=", alterSQLs)
		} else {
			log.Println("[Debug] check index.alter ", fmt.Sprintf("%s.%s", table, indexName), "not change")
		}
	}

	// drop index
	if sc.Config.Drop {
		for indexName, dIdx := range destMyS.IndexAll {
			if sc.Config.IsIgnoreIndex(table, indexName) {
				log.Printf("ignore index %s.%s", table, indexName)
				continue
			}
			var dropSQL string
			if _, has := sourceMyS.IndexAll[indexName]; !has {
				dropSQL = dIdx.alterDropSQL()
			}

			if dropSQL != "" {
				alterLines = append(alterLines, dropSQL)
				logm.Println("[Debug] check index.drop ", fmt.Sprintf("%s.%s", table, indexName), "alterSQL=", dropSQL)
			} else {
				log.Println("[Debug] check index.drop ", fmt.Sprintf("%s.%s", table, indexName), " not change")
			}
		}
	}

	// 比对外键
	for foreignName, idx := range sourceMyS.ForeignAll {
		if sc.Config.IsIgnoreForeignKey(table, foreignName) {
			log.Printf("ignore foreignName %s.%s", table, foreignName)
			continue
		}
		dIdx, has := destMyS.ForeignAll[foreignName]
		//logm.Println("[Debug] foreignName---->[", fmt.Sprintf("%s.%s", table, foreignName), "] dest_has:", has, "\ndest_idx:", dIdx, "\nsource_idx:", idx)
		var alterSQLs []string
		if has {
			if idx.SQL != dIdx.SQL {
				alterSQLs = append(alterSQLs, idx.alterAddSQL(true)...)
			}
		} else {
			alterSQLs = append(alterSQLs, idx.alterAddSQL(false)...)
		}
		if len(alterSQLs) > 0 {
			alterLines = append(alterLines, alterSQLs...)
			logm.Println("[Debug] check foreignKey.alter ", fmt.Sprintf("%s.%s", table, foreignName), "alterSQL=", alterSQLs)
		} else {
			log.Println("[Debug] check foreignKey.alter ", fmt.Sprintf("%s.%s", table, foreignName), "not change")
		}
	}

	// drop 外键
	if sc.Config.Drop {
		for foreignName, dIdx := range destMyS.ForeignAll {
			if sc.Config.IsIgnoreForeignKey(table, foreignName) {
				log.Printf("ignore foreignName %s.%s", table, foreignName)
				continue
			}
			var dropSQL string
			if _, has := sourceMyS.ForeignAll[foreignName]; !has {
				log.Println("[Debug] foreignName --->[", fmt.Sprintf("%s.%s", table, foreignName), "]", "didx:", dIdx)
				dropSQL = dIdx.alterDropSQL()

			}
			if dropSQL != "" {
				alterLines = append(alterLines, dropSQL)
				logm.Println("[Debug] check foreignKey.drop ", fmt.Sprintf("%s.%s", table, foreignName), "alterSQL=", dropSQL)
			} else {
				log.Println("[Debug] check foreignKey.drop ", fmt.Sprintf("%s.%s", table, foreignName), "not change")
			}
		}
	}

	return alterLines
}

// SyncSQL4Dest sync schema change
func (sc *SchemaSync) SyncSQL4Dest(sqlStr string, sqls []string) error {
	log.Print("Exec_SQL_START:\n>>>>>>\n", sqlStr, "\n<<<<<<<<\n\n")
	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" {
		log.Println("sql_is_empty, skip")
		return nil
	}
	t := newMyTimer()
	ret, err := sc.DestDb.Query(sqlStr)

	// how to enable allowMultiQueries?
	if err != nil && len(sqls) > 1 {
		log.Println("exec_mut_query failed, err=", err, ",now exec SQLs foreach")
		tx, errTx := sc.DestDb.Db.Begin()
		if errTx == nil {
			for _, sql := range sqls {
				ret, err = tx.Query(sql)
				log.Println("query_one:[", sql, "]", err)
				if err != nil {
					break
				}
			}
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
		}
	}
	t.stop()
	if err != nil {
		log.Println("EXEC_SQL_FAILED:", err)
		return err
	}
	log.Println("EXEC_SQL_SUCCESS, used:", t.usedSecond())
	cl, err := ret.Columns()
	log.Println("EXEC_SQL_RET:", cl, err)
	return err
}

func CheckSchemaDiffStart(cfg *Config) {
	scs := newStatics(cfg)
	defer func() {
		scs.timer.stop()
		scs.sendMailNotice(cfg)
	}()

	//连接数据库
	sc := NewSchemaSync(cfg)
	//获取db状态,判断是否存在
	sc.GetDatabase(cfg)

	//生成的sql文件中,开始位置添加database名称
	fmtm.Println("-- Database")
	fmtm.Println("use `" + sc.DestDb.dbName + "`;")

	//对比表[table]
	CheckSchemaDiff(scs, cfg, sc, TYPE_TABLE)
	//对比视图[view]
	CheckSchemaDiff(scs, cfg, sc, TYPE_VIEW)
}

// CheckSchemaDiff 执行最终的diff
func CheckSchemaDiff(scs *statics, cfg *Config, sc *SchemaSync, tType string) {

	newTables := sc.GetTableNames(tType)
	// log.Println("source db table total:", len(newTables))

	changedTables := make(map[string][]*TableAlterData)

	for _, table := range newTables {
		// log.Printf("Index : %d Table : %s\n", index, table)
		if !cfg.CheckMatchTables(table) {
			// log.Println("Table:", table, "skip")
			continue
		}

		if cfg.CheckMatchIgnoreTables(table) {
			log.Println("Table:", table, "skipped by ignore")
			continue
		}

		sd := sc.getAlterDataByTable(table, cfg, tType)

		if sd.Type == alterTypeNo {
			//log.Println("table:", table, "not change,", sd)
			log.Println("table:", table, "not change")
			continue
		}

		//if sd.Type == alterTypeDropTable {
		//	log.Println("skipped table", table, ",only exists in dest's db")
		//	continue
		//}

		//sql中添加database名称
		var sqls []string
		for _, sql := range sd.SQL {
			sql := strings.Replace(sql, "`"+table+"`", sc.DestDb.dbName+".`"+table+"`", -1)
			sqls = append(sqls, sql)
		}
		sd.SQL = sqls

		//打印sql
		fmtm.Println(sd)
		relationTables := sd.SchemaDiff.RelationTables()
		// fmt.Println("relationTables:",table,relationTables)

		// 将所有有外键关联的单独放
		groupKey := "multi"
		if len(relationTables) == 0 {
			groupKey = "single_" + table
		}
		if _, has := changedTables[groupKey]; !has {
			changedTables[groupKey] = make([]*TableAlterData, 0)
		}
		changedTables[groupKey] = append(changedTables[groupKey], sd)
	}

	//log.Println("[Debug] changedTables:", changedTables)

	countSuccess := 0
	countFailed := 0
	canRunTypePref := "single"
	// 先执行单个表的
run_sync:
	for typeName, sds := range changedTables {
		if !strings.HasPrefix(typeName, canRunTypePref) {
			continue
		}
		//log.Println("runSyncType:", typeName)
		var sqls []string
		var sts []*tableStatics
		for _, sd := range sds {
			for index := range sd.SQL {
				sql := strings.TrimRight(sd.SQL[index], ";")
				sqls = append(sqls, sql)

				st := scs.newTableStatics(sd.Table, sd)
				sts = append(sts, st)
			}
		}

		sql := strings.Join(sqls, ";\n") + ";"
		var ret error

		if sc.Config.Sync {

			ret = sc.SyncSQL4Dest(sql, sqls)
			if ret == nil {
				countSuccess++
			} else {
				countFailed++
			}
		}
		for _, st := range sts {
			st.alterRet = ret
			st.schemaAfter = sc.DestDb.GetTableSchema(st.table, tType)
			st.timer.stop()
		}

	} // end for

	// 最后再执行多个表的alter
	if canRunTypePref == "single" {
		canRunTypePref = "multi"
		goto run_sync
	}

	if sc.Config.Sync {
		log.Println("execute_all_sql_done, success_total:", countSuccess, "failed_total:", countFailed)
	}

}
