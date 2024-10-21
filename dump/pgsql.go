package dump

import (
	"compress/gzip"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
)

type PgsqlDumper struct {
	Host       string
	Port       int
	UserName   string
	PassWord   string
	DataBase   string
	db         *sql.DB
	gzipWriter *gzip.Writer
}

func (p *PgsqlDumper) connectDB() (err error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", p.Host, p.Port, p.UserName, p.PassWord, p.DataBase)
	if p.db, err = sql.Open("postgres", psqlInfo); err != nil {
		return err
	}
	return nil
}

func (p *PgsqlDumper) getSchemaName() (schemaNames []string, err error) {
	var row *sql.Rows
	if row, err = p.db.Query("SELECT DISTINCT schemaname FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')"); err != nil {
		return schemaNames, err
	}
	for row.Next() {
		var schemaName string
		if row.Scan(&schemaName); err != nil {
			return schemaNames, err
		}
		//写入创建schema数据库命令
		p.gzipWriter.Write([]byte(fmt.Sprintf("CREATE CREATE SCHEMA %s;\n", schemaName)))
		schemaNames = append(schemaNames, schemaName)
	}
	return schemaNames, nil
}

func (p *PgsqlDumper) getTables(schemaName string) (tables []string, err error) {
	var rows *sql.Rows
	if rows, err = p.db.Query("SELECT tablename FROM pg_tables WHERE schemaname = '%s'", schemaName); err != nil {
		return tables, err
	}

	for rows.Next() {
		var tableName string
		if err = rows.Scan(&tableName); err != nil {
			return tables, err
		}
		tables = append(tables, tableName)
	}
	rows.Close()
	return tables, nil
}

func (p *PgsqlDumper) getCreateTableStatement(tableName string) (err error) {
	var rows *sql.Rows
	query := fmt.Sprintf("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '%s'", tableName)
	if rows, err = p.db.Query(query); err != nil {
		return err
	}

	var columns []string
	for rows.Next() {
		var columnName, dataType string
		var charMaxLength *int
		if err = rows.Scan(&columnName, &dataType, &charMaxLength); err != nil {
			return err
		}
		columnDefine := fmt.Sprintf("%s %s", columnName, dataType)
		if charMaxLength != nil {
			columnDefine += fmt.Sprintf("(%d)", *charMaxLength)
		}
		columns = append(columns, columnDefine)
	}
	rows.Close()
	p.gzipWriter.Write([]byte(fmt.Sprintf("CREATE TABLE %s (\n    %s\n);", tableName, strings.Join(columns, ",\n    "))))
	return err
}

func (p *PgsqlDumper) getPrimaryKey(tableName, schemaName string) (primaryKey string, minPrimaryKey, maxPrimaryKey int, err error) {
	query := fmt.Sprintf("SELECT a.attname AS column_name FROM  pg_constraint con JOIN pg_class rel ON rel.oid = con.conrelid JOIN  pg_namespace nsp ON nsp.oid = rel.relnamespace JOIN  pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey) WHERE   con.contype = 'p' AND rel.relname = '%s' AND nsp.nspname = '%s'", tableName, schemaName)
	if err = p.db.QueryRow(query).Scan(&primaryKey); err != nil {
		return primaryKey, minPrimaryKey, maxPrimaryKey, err
	}
	if err = p.db.QueryRow(fmt.Sprintf("SELECT min(%s) FROM %s", primaryKey, tableName)).Scan(&minPrimaryKey); err != nil {
		return primaryKey, minPrimaryKey, maxPrimaryKey, err
	}
	if err = p.db.QueryRow(fmt.Sprintf("SELECT max(%s) FROM %s", primaryKey, tableName)).Scan(&maxPrimaryKey); err != nil {
		return primaryKey, minPrimaryKey, maxPrimaryKey, err
	}
	return primaryKey, minPrimaryKey, maxPrimaryKey, err
}
