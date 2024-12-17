package dump

import (
	"compress/gzip"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"strings"
)

type MysqlDumper struct {
	Host       string
	Port       int
	UserName   string
	PassWord   string
	DataBase   string
	db         *sql.DB
	GzipWriter *gzip.Writer
}

const (
	varchar = "varchar"
)

func (m *MysqlDumper) dump() (err error) {
	//连接数据库
	var tableStructure string
	var primaryKey string
	var minPrimaryKey int
	var maxPrimaryKey int
	var dataType string
	var tables []string

	if err = m.connectDB(); err != nil {
		return err
	}
	//获取所有表名
	if tables, err = m.getTables(); err != nil {
		fmt.Println(err.Error())
		return err
	}
	//获取创建表的sql语句

	for _, table := range tables {

		if !m.CheckIsNull(table) {
			continue
		}
		if tableStructure, err = m.getTableStructure(table); err != nil {
			return
		}
		//写入表结构
		m.GzipWriter.Write([]byte(fmt.Sprintf("-- Table structure for `%s`\n%s;\n\n", table, tableStructure)))
		if primaryKey, dataType, minPrimaryKey, maxPrimaryKey, err = m.getPrimaryKey(table); err != nil || primaryKey == "" {
			fmt.Printf("正在导出%s,使用select *导出\n", table)
			m.getDataByLimit(table)
		} else {
			//fmt.Println(primaryKey, dataType, minPrimaryKey, maxPrimaryKey, table)
			fmt.Printf("正在导出%s,采用主键模式,键值为%s,%s,%d,%d\n", table, primaryKey, dataType, minPrimaryKey, maxPrimaryKey)

			m.getDataByPrimaryKey(table, primaryKey, dataType, minPrimaryKey, maxPrimaryKey)
		}

	}

	return nil
}
func (m *MysqlDumper) CheckIsNull(table string) (NotNull bool) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	m.db.QueryRow(query).Scan(&count)
	if count != 0 {
		NotNull = true
	}
	return NotNull
}

func (m *MysqlDumper) connectDB() error {
	var err error
	m.db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", m.UserName, m.PassWord, m.Host, m.Port, m.DataBase))
	if err != nil {
		return err
	}
	return nil
}

func (m *MysqlDumper) getTables() (tables []string, err error) {
	var rows *sql.Rows
	if rows, err = m.db.Query("SHOW TABLES"); err != nil {
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

func (m *MysqlDumper) getTableStructure(tableName string) (createTableStmt string, err error) {
	err = m.db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", tableName)).Scan(&tableName, &createTableStmt)
	if err != nil {
		return "", err
	}
	return createTableStmt, nil
}

func (m *MysqlDumper) getPrimaryKey(tableName string) (primaryKey, dataType string, minPrimaryKey, maxPrimaryKey int, err error) {
	//获取主键
	if err = m.db.QueryRow(fmt.Sprintf(`
			SELECT GROUP_CONCAT(COLUMN_NAME)
			FROM information_schema.KEY_COLUMN_USAGE
			WHERE TABLE_NAME = '%s' AND TABLE_SCHEMA = '%s' AND CONSTRAINT_NAME = 'PRIMARY'`, tableName, m.DataBase)).Scan(&primaryKey); err != nil {
		return primaryKey, dataType, minPrimaryKey, maxPrimaryKey, err
	}

	if err = m.db.QueryRow(fmt.Sprintf("SELECT min(%s) from %s", primaryKey, tableName)).Scan(&minPrimaryKey); err != nil {

		return primaryKey, dataType, minPrimaryKey, maxPrimaryKey, err
	}
	if err = m.db.QueryRow(fmt.Sprintf("SELECT max(%s) from %s", primaryKey, tableName)).Scan(&maxPrimaryKey); err != nil {
		return primaryKey, dataType, minPrimaryKey, maxPrimaryKey, err
	}

	//if err = m.db.QueryRow(fmt.Sprintf("SELECT max(%s) from %s", primaryKey, tableName)).Scan(&maxPrimaryKey); err != nil {
	//	return primaryKey, dataType, minPrimaryKey, maxPrimaryKey, err
	//}

	if err = m.db.QueryRow(fmt.Sprintf("SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = '%s' AND TABLE_SCHEMA = '%s' AND COLUMN_KEY = 'PRI'", tableName, m.DataBase)).Scan(&dataType); err != nil {

		return primaryKey, dataType, minPrimaryKey, maxPrimaryKey, err
	}
	primaryKey = strings.Split(primaryKey, ",")[0]
	return primaryKey, dataType, minPrimaryKey, maxPrimaryKey, err

}

func (m *MysqlDumper) getDataByPrimaryKey(table, primaryKey, dataType string, minPrimaryKey, maxPrimaryKey int) {
	var err error
	total := (maxPrimaryKey-minPrimaryKey+1)/1000 + 1
	startPrimaryKey := minPrimaryKey / 1000
	for i := 0; i < total; i++ {
		var rows *sql.Rows
		var querySql string
		start := (startPrimaryKey + i) * 1000
		end := (startPrimaryKey + i + 1) * 1000

		switch dataType {
		case varchar:
			querySql = fmt.Sprintf("SELECT * FROM %s WHERE '%s' > %d and '%s' < %d", table, primaryKey, start, primaryKey, end)
		default:
			querySql = fmt.Sprintf("SELECT * FROM %s WHERE %s > %d and %s < %d", table, primaryKey, start, primaryKey, end)
		}
		if rows, err = m.db.Query(querySql); err != nil {
			fmt.Println("查询失败:", err.Error(), "查询语句:", querySql)
			return
		}
		if _, err = m.dealRows(rows, table); err != nil {
			continue
		}
	}
}

func (m *MysqlDumper) getDataByLimit(table string) {
	pageSize := 1000 // 每页查询1000条记录
	offset := 0      // 偏移量初始为0
	for {
		// 分页查询数据
		var rows *sql.Rows
		var err error
		var isNull bool
		querySql := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table, pageSize, offset)
		if rows, err = m.db.Query(querySql); err != nil {
			fmt.Println("查询失败:", err.Error(), "查询语句:", querySql)
			return
		}
		if isNull, err = m.dealRows(rows, table); err != nil {
			continue
		}
		if isNull {
			break
		}

		// 处理完一页，增加 offset
		offset += pageSize
	}
}

func (m *MysqlDumper) dealRows(rows *sql.Rows, table string) (isNull bool, err error) {
	var columns []string

	var allData []string

	if columns, err = rows.Columns(); err != nil {
		fmt.Println("获取列失败:", err)
		return isNull, err
	}

	for i, col := range columns {
		columns[i] = fmt.Sprintf("`%s`", col) // 使用反引号
	}

	// 处理每一行数据
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			fmt.Println("扫描行数据失败:", err)
			return isNull, err
		}

		// 将每行数据转成 SQL 插入语句
		valueStrings := make([]string, len(values))
		for i, val := range values {
			if val == nil {
				valueStrings[i] = "NULL"
			} else {
				//处理sql注入导致数据内容有特殊字符
				content := strconv.Quote(string(val))
				//fmt.Println(content)
				//content := strings.ReplaceAll(string(val), `\'`, `'`)
				//content = strings.ReplaceAll(string(val), `'`, `\'`)
				valueStrings[i] = fmt.Sprintf("'%s'", content)
			}
		}

		allData = append(allData, fmt.Sprintf("(%s)", strings.Join(valueStrings, ",")))
	}
	rows.Close()
	if len(allData) != 0 {
		insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;\n", table, strings.Join(columns, ", "), strings.Join(allData, ", "))

		// 将插入语句写入 gzip 文件
		_, err = m.GzipWriter.Write([]byte(insertQuery))
		if err != nil {
			fmt.Println("写入文件失败:", err)
			return isNull, err
		}
	} else {
		isNull = true
	}
	// 如果当前页查询没有返回任何数据，表示导出结束

	return isNull, err
}
