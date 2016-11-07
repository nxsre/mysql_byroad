package main

import (
	"flag"
	"fmt"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

var createSql = `CREATE TABLE user (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(10) NOT NULL,
    password VARCHAR(10) NOT NULL,
    age INTEGER NOT NULL,
    address VARCHAR(20) NOT NULL,
    email VARCHAR(20) NOT NULL
)`

func main() {
	count := flag.Int64("n", 1000, "number of sql")
	flag.Parse()
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/test", "root", "", "127.0.0.1", 3306)
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	db.MustExec("DROP TABLE IF EXISTS user")
	db.MustExec(createSql)
	var i int64
	for i = 0; i < *count; i++ {
		insert(db)
	}
	for i = 1; i <= *count; i++ {
		update(db, i)
	}
}

func insert(db *sqlx.DB) (int64, error) {
	sql := "INSERT INTO user (username, password, age, address, email) VALUES (?, ?, ?, ?, ?)"
	rows, err := db.Exec(sql, "yangxin", "iampastor", 10, "chengdu", "pastor.xin@gmail.com")
	if err != nil {
		fmt.Println(err.Error())
		return 0, err
	}
	return rows.LastInsertId()
}

func update(db *sqlx.DB, id int64) (int64, error) {
	sql := "UPDATE `user` SET `username`=?,`password`=?,`age`=?,`address`=?,`email`=? WHERE `id`=?"
	rows, err := db.Exec(sql, "xinyang", "bounce", 20, "beijin", "yangxin5_21@163.com", id)
	if err != nil {
		fmt.Println(err.Error())
		return 0, err
	}
	return rows.RowsAffected()
}
