package main

import "github.com/BurntSushi/toml"

type Config struct {
	MysqlConfs    []MysqlConf   `toml:"mysql"`
	MonitorConf   MonitorConf   `toml:"monitor"`
	RPCClientConf RPCClientConf `toml:"rpc_client"`
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

type RPCClientConf struct {
	Host string
	Port int
}

type NSQConf struct {
	LookupdHttpAddr string   `toml:"lookupd_http_address"`
	NsqdAddrs       []string `toml:"nsqd_tcp_address"`
}

var Conf Config

func init() {
	_, err := toml.DecodeFile("config.toml", &Conf)
	if err != nil {
		panic(err)
	}
}
