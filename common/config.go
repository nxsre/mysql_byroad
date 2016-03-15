package common

import (
	"github.com/Unknwon/goconfig"
)

type Configer struct {
	configer *goconfig.ConfigFile
}

type AuthConfig struct {
	AuthUrl string
	AppKey  string
	AppName string
}

type MysqlConfig struct {
	Username string
	Password string
	Host     string
	Port     int
}

type RedisConfig struct {
	Host      string
	Port      string
	Password  string
	MaxIdle   int
	MaxActive int
}

type LogConfig struct {
	ErrLogPath string
	SysLogPath string
}

type SysConfig struct {
	ServerID       uint32
	UpdateDuration int
}

type WebConfig struct {
	Host string
	Port string
}

type RPCClientConfig struct {
	Schema string
}

type RPCServerConfig struct {
	Schema string
	Desc   string
}

type OWLConfig struct {
	App string
	IP  string
}

func NewConfiger(filename string) (c *Configer, err error) {
	cf, err := goconfig.LoadConfigFile(filename)
	if err != nil {
		return nil, err
	}
	configer := Configer{
		configer: cf,
	}
	return &configer, nil
}

func (this *Configer) GetString(section, key string, defaultValue ...string) string {
	return this.configer.MustValue(section, key, defaultValue...)
}

func (this *Configer) GetInt(section, key string, defaultValue ...int) int {
	return this.configer.MustInt(section, key, defaultValue...)
}

func (this *Configer) GetArray(section, key, delim string) []string {
	return this.configer.MustValueArray(section, key, delim)
}

func (this *Configer) GetAuth() *AuthConfig {
	ac := AuthConfig{
		AuthUrl: this.GetString("auth", "auth_url"),
		AppKey:  this.GetString("auth", "appkey"),
		AppName: this.GetString("auth", "appname"),
	}
	return &ac
}

func (this *Configer) GetMysql() *MysqlConfig {
	mc := MysqlConfig{
		Username: this.GetString("mysql", "server_username"),
		Password: this.GetString("mysql", "server_password"),
		Host:     this.GetString("mysql", "server_host"),
		Port:     this.GetInt("mysql", "server_port"),
	}
	return &mc
}

func (this *Configer) GetRedis() *RedisConfig {
	rc := RedisConfig{
		Host:      this.GetString("redis", "host"),
		Port:      this.GetString("redis", "port"),
		Password:  this.GetString("redis", "password"),
		MaxIdle:   this.GetInt("redis", "max_idle"),
		MaxActive: this.GetInt("redis", "max_active"),
	}
	return &rc
}

func (this *Configer) GetLog() *LogConfig {
	lc := LogConfig{
		ErrLogPath: this.GetString("log", "err_log_path"),
		SysLogPath: this.GetString("log", "sys_log_path"),
	}
	return &lc
}

func (this *Configer) GetSys() *SysConfig {
	sc := SysConfig{
		ServerID:       uint32(this.GetInt("system", "server_id")),
		UpdateDuration: this.GetInt("system", "config_update_duration"),
	}
	return &sc
}

func (this *Configer) GetWeb() *WebConfig {
	wc := WebConfig{
		Host: this.GetString("web", "host", "0.0.0.0"),
		Port: this.GetString("web", "port", "4000"),
	}
	return &wc
}

func (this *Configer) GetRPCClients() []*RPCClientConfig {
	schemas := this.GetArray("rpc_client", "schemas", " ")
	configs := make([]*RPCClientConfig, 0, 10)
	for _, schema := range schemas {
		conf := RPCClientConfig{
			Schema: schema,
		}
		configs = append(configs, &conf)
	}
	return configs
}

func (this *Configer) GetRPCServer() *RPCServerConfig {
	schema := this.GetString("rpc_server", "schema")
	desc := this.GetString("rpc_server", "description")
	rc := RPCServerConfig{
		Schema: schema,
		Desc:   desc,
	}
	return &rc
}

func (this *Configer) GetOWL() *OWLConfig {
	ip := this.GetString("OWL", "ip")
	app := this.GetString("OWL", "app")
	return &OWLConfig{
		App: app,
		IP:  ip,
	}
}
