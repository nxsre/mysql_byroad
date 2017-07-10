package main

import (
	"fmt"
	"log"

	"flag"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

//mysql -h10.0.12.44 -P6006 -ubyroad_swd -p0CX9VnXh
var (
	Username = "byroad_swd"
	Password = "0CX9VnXh"
	Host     = "10.17.30.19"
	Port     = 6006
	DBName   = "byroad"
)

/*var (
	Username = "root"
	Password = "123456"
	Host     = "172.20.4.102"
	Port     = 3306
	DBName   = "byroad"
)*/

var dbs = []string{
	"activities",
	"cms",
	"jumei",
	"jumei_cart",
	"jumei_ofs",
	"jumei_orders_sharding1",
	"jumei_orders_sharding2",
	"jumei_orders_sharding3",
	"jumei_orders_sharding4",
	"jumei_shipings_sharding1",
	"jumei_shipings_sharding2",
	"jumei_shipings_sharding3",
	"jumei_shipings_sharding4",
	"mobile",
	"payments_1",
	"payments_2",
	"payments_3",
	"payments_4",
	"tuanmei",
	"usercenter",
	"usercenter_sharding1",
	"usercenter_sharding2",
	"usercenter_sharding3",
	"usercenter_sharding4",
}

func main() {
	flag.Parse()
	from := flag.Arg(0)
	to := flag.Arg(1)
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true",
		Username, Password, Host, Port,
		DBName)
	log.Printf("connect to %s\n", dsn)
	db := sqlx.MustOpen("mysql", dsn)
	for _, dbname := range dbs {
		from = dbname
		to = dbname + "_cl"
		s := fmt.Sprintf("update task set db_instance_name=? where db_instance_name=?")
		log.Println(s, to, from)
		_, err := db.Exec(s, to, from)
		if err != nil {
			log.Fatalf(err.Error())
		}
		s = fmt.Sprintf("update task set stat=? where db_instance_name=?")
		log.Println(s, "停止", to)
		_, err = db.Exec(s, "停止", to)
		if err != nil {
			log.Fatalf(err.Error())
		}
		/*s = fmt.Sprintf("update config set description=? where description=?")
		log.Println(s)
		_, err = db.Exec(s, to, from)
		if err != nil {
			log.Fatalf(err.Error())
		}*/
	}
}
