package main

import (
	"database/sql"
	"flag"
	"mysql_byroad/model"
	"time"

	"github.com/BurntSushi/toml"
	log "github.com/Sirupsen/logrus"
	"github.com/jmoiron/sqlx"
)

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type Config struct {
	DBConfig        DBConfig      `toml:"db_config"`
	Logfile         string        `toml:"logfile"`
	BinlogInterval  duration      `toml:"binlog_flush_interval"`
	RPCPingInterval duration      `toml:"rpc_ping_interval"`
	MysqlConf       MysqlConf     `toml:"mysql"`
	MonitorConf     MonitorConf   `toml:"monitor"`
	RPCServerConf   RPCServerConf `toml:"rpc_server"`
	NSQConf         NSQConf       `toml:"nsq"`
	LogLevel        string        `toml:"loglevel"`
	DBInstanceName  string        `toml:"db_instance_name"`
}

type MysqlConf struct {
	Name           string
	ServerId       uint32 `toml:"server_id"`
	Host           string
	Port           uint16
	Username       string
	Password       string
	BinlogFilename string `toml:"binlog_filename"`
	BinlogPosition uint32 `toml:"binlog_position"`
	Exclude        []string
}

type MonitorConf struct {
	Host    string
	RpcPort int `toml:"rpc_port"`
}

type RPCServerConf struct {
	Host string
	Port int
	Desc string
}

type NSQConf struct {
	LookupdHttpAddrs []string `toml:"lookupd_http_address"`
	NsqdAddrs        []string `toml:"nsqd_tcp_address"`
	LookupInterval   duration `toml:"lookup_interval"`
}

type DBConfig struct {
	Host     string
	Port     uint16
	Username string
	Password string
	DBName   string `toml:"dbname"`
}

func InitConfig() *Config {
	config := Config{}
	configFile := ParseConfig()
	_, err := toml.DecodeFile(configFile, &config)
	if err != nil {
		panic(err)
	}
	return &config
}

func ParseConfig() string {
	filename := flag.String("c", "dispatcher.toml", "config file path")
	flag.Parse()
	return *filename
}

type ConfigDB struct {
	db  *sqlx.DB
	dsn string
}

func NewConfigDB(dsn string) (*ConfigDB, error) {
	confdb := &ConfigDB{}
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	model.Init(db)
	model.CreateConfigTable()
	confdb.db = db
	confdb.dsn = dsn
	return confdb, nil
}

func (confdb *ConfigDB) SaveConfig(key, value, desc string) (int64, error) {
	var cnt int64
	err := confdb.db.Get(&cnt, "SELECT COUNT(*) FROM config WHERE `key`=? AND `description`=?", key, desc)
	if err != nil {
		log.Error("save config error1:", err.Error())
		return 0, err
	}
	var res sql.Result
	if cnt == 0 {
		res, err = confdb.db.Exec("INSERT INTO config(`key`, `value`, `description`) VALUES(?, ?, ?)", key, value, desc)
		if err != nil {
			log.Error("save config error2:", err.Error())
			return 0, err
		}
		return res.LastInsertId()
	} else {
		res, err = confdb.db.Exec("UPDATE config SET `value`=? WHERE `key`=? AND `description`=?", value, key, desc)
		if err != nil {
			log.Error("save config error3:", err.Error())
			return 0, err
		}
		return res.RowsAffected()
	}
}

func (confdb *ConfigDB) GetConfig(key, desc string) (string, error) {
	var value string
	err := confdb.db.Get(&value, "SELECT `value` FROM config WHERE `key`=? AND `description`=?", key, desc)
	return value, err
}
