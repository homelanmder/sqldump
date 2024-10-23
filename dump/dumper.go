package dump

import (
	"compress/gzip"
	"fmt"
	"os"
	"time"
)

type Dumper struct {
	Host       string
	Port       int
	UserName   string
	PassWord   string
	DataBase   string
	Driver     string
	gzipWriter *gzip.Writer
}

const (
	mysql = "mysql"
	pgsql = "pgsql"
)

func (d *Dumper) NewDumper(host string, port int, username, password, dataBase, driver string) *Dumper {
	return &Dumper{Host: host, Port: port, UserName: username, PassWord: password, DataBase: dataBase, Driver: driver}
}

func (d *Dumper) Dump() (err error) {
	var sqlFile *os.File
	if sqlFile, err = os.Create(fmt.Sprintf("%s_%s.sql.gz", d.DataBase, time.Now().Format("2006_01_15_04"))); err != nil {
		return err
	}
	d.gzipWriter = gzip.NewWriter(sqlFile)
	defer sqlFile.Close()
	defer d.gzipWriter.Close()
	switch d.Driver {
	case mysql:
		mysqlDumper := MysqlDumper{
			Host:       d.Host,
			Port:       d.Port,
			UserName:   d.UserName,
			PassWord:   d.PassWord,
			DataBase:   d.DataBase,
			GzipWriter: d.gzipWriter,
		}
		if err = mysqlDumper.dump(); err != nil {
			return err
		}
	case pgsql:
	default:

	}
	return err
}
