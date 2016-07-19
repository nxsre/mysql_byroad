package main

import (
	"mysql_byroad/common"
	"time"

	"github.com/BurntSushi/toml"
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
	Debug                   bool
	RPCClientLookupInterval duration      `toml:"rpcclient_lookup_interval"`
	NSQAdminHttpAddress     string        `toml:"nsqadmin_http_address"`
	NSQLookupdAddress       []string      `toml:"nsqlookupd_http_address"`
	MysqlConf               MysqlConf     `toml:"mysql"`
	RPCServerConf           RPCServerConf `toml:"rpcserver"`
	WebConfig               WebConfig     `toml:"web"`
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
