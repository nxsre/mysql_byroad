package mysql

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

type MysqlConfig struct {
	Name     string
	Host     string
	Port     uint16
	Username string
	Password string
	include  []string
	exclude  []string
}

type Inspector struct {
	config    *MysqlConfig
	db        *sqlx.DB
	columnMap ColumnMap
}

func NewInspector(config *MysqlConfig) (*Inspector, error) {
	inspector := Inspector{
		config: config,
	}
	db, err := inspector.connect()
	if err != nil {
		return &inspector, err
	}
	inspector.db = db
	return &inspector, nil
}

func (this *Inspector) connect() (*sqlx.DB, error) {
	config := this.config
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/information_schema", config.Username, config.Password, config.Host, config.Port)
	db, err := sqlx.Open("mysql", dsn)
	return db, err
}

/*
开始定时刷新字段信息
*/
func (this *Inspector) InspectLoop() {

}
