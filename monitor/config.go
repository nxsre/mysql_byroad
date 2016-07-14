package main

import (
	"mysql_byroad/common"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Debug         bool
	MysqlConf     MysqlConf     `toml:"mysql"`
	RPCServerConf RPCServerConf `toml:"rpcserver"`
	WebConfig     WebConfig     `toml:"web"`
}
type MysqlConf struct {
	Host     string
	Port     uint16
	Username string
	Password string
	DBName   string `toml:"dbname"`
}

type RPCServerConf struct {
	Host string
	Port int
}

type WebConfig struct {
	Host    string
	Port    int
	AuthURL string `toml:"auth_url"`
	AppKey  string `toml:"appkey"`
	AppName string `toml:"appname"`
}

var Conf Config

func init() {
	configFile := common.ParseConfig()
	_, err := toml.DecodeFile(configFile, &Conf)
	if err != nil {
		panic(err)
	}
}
