package main

import "github.com/BurntSushi/toml"

type Config struct {
	MysqlConf      MysqlConf      `toml:"mysql"`
	RPCServerConf  RPCServerConf  `toml:"rpcserver"`
	DispatcherConf DispatcherConf `toml:"dispatcher"`
	WebConfig      WebConfig      `toml:"web"`
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

type DispatcherConf struct {
	Host    string
	RPCPort int
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
	_, err := toml.DecodeFile("config.toml", &Conf)
	if err != nil {
		panic(err)
	}
}
