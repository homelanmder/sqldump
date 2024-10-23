package main

import (
	"flag"
	"fmt"
	"sqldump/dump"
)

func main() {
	var username string
	var password string
	var driver string
	var database string
	var host string
	var port int
	flag.StringVar(&username, "u", "", "指定用户名")
	flag.StringVar(&password, "p", "", "指定密码")
	flag.StringVar(&driver, "driver", "mysql", "指定数据库类型")
	flag.StringVar(&host, "h", "", "指定主机")
	flag.IntVar(&port, "port", 3306, "指定端口")
	flag.StringVar(&database, "d", "", "指定数据库")

	flag.Parse()
	if username == "" || password == "" || host == "" || database == "" {
		flag.Usage()
		return
	}
	dumper := dump.Dumper{
		Host:     host,
		UserName: username,
		PassWord: password,
		Port:     port,
		Driver:   driver,
		DataBase: database,
	}
	if err := dumper.Dump(); err != nil {
		fmt.Println(err.Error())
		return
	}

}
