package main

import (
	"database/sql"
	"epg/log"
	"time"

	"mysql_byroad/common"

	"github.com/BurntSushi/toml"
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
	ConfigDB        string        `toml:"config_db"`
	BinlogInterval  duration      `toml:"binlog_flush_interval"`
	RPCPingInterval duration      `toml:rpc_ping_interval`
	MysqlConf       MysqlConf     `toml:"mysql"`
	MonitorConf     MonitorConf   `toml:"monitor"`
	RPCServerConf   RPCServerConf `toml:"rpc_server"`
	NSQConf         NSQConf       `toml:"nsq"`
}

type MysqlConf struct {
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
}

var Conf Config

func init() {
	configFile := common.ParseConfig()
	_, err := toml.DecodeFile(configFile, &Conf)
	if err != nil {
		panic(err)
	}
}

func InitConfigDB() error {
	var err error
	confdb, err = sqlx.Open("sqlite3", Conf.ConfigDB)
	if err != nil {
		panic(err)
	}
	s := "CREATE TABLE IF NOT EXISTS `config` ( " +
		"`id` INTEGER PRIMARY KEY AUTOINCREMENT," +
		"`key` varchar(120) NOT NULL," +
		"`value` varchar(120) NOT NULL," +
		"`description` varchar(120)" +
		")"
	_, err = confdb.Exec(s)
	return err
}

func SaveConfig(key, value, desc string) (int64, error) {
	var cnt int64
	err := confdb.Get(&cnt, "SELECT COUNT(*) FROM config WHERE key=?", key)
	if err != nil {
		log.Error("save config error:", err.Error())
		return 0, err
	}
	var res sql.Result
	if cnt == 0 {
		res, err = confdb.Exec("INSERT INTO config(key, value, description) VALUES(?, ?, ?)", key, value, desc)
		if err != nil {
			log.Error("save config error:", err.Error())
			return 0, err
		}
		return res.LastInsertId()
	} else {
		res, err = confdb.Exec("UPDATE config SET value=?, description=? WHERE key=?", value, desc, key)
		if err != nil {
			log.Error("save config error:", err.Error())
			return 0, err
		}
		return res.RowsAffected()
	}
}

func GetConfig(key string) string {
	var value string
	confdb.Get(&value, "SELECT value FROM config WHERE key=?", key)
	log.Debugf("get config %s: %s", key, value)
	return value
}
