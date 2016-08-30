package main

import (
	"fmt"
	"mysql_byroad/model"
	"mysql_byroad/mysql_schema"
	"mysql_byroad/nsq"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/jmoiron/sqlx"
)

var (
	pusherManager     *PusherManager
	dispatcherManager *DispatcherManager
	rpcServer         *Monitor
	nsqManager        *nsqm.NSQManager
	columnManager     *schema.ColumnManager
)

func main() {
	var err error
	InitLog()
	log.Debugf("Conf: %+v", Conf)
	pusherManager = NewPusherManager()
	dispatcherManager = NewDispatcherManager()
	rpcServer = NewRPCServer("tcp", fmt.Sprintf("%s:%d", Conf.RPCServerConf.Host, Conf.RPCServerConf.Port), "")
	rpcServer.start()
	nsqManager, err = nsqm.NewNSQManager(Conf.NSQLookupdAddress, nil, nil)
	if err != nil {
		log.Error("new nsq manager error: ", err.Error())
	}
	nsqManager.NodeInfoUpdateLoop()

	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true",
		Conf.MysqlConf.Username, Conf.MysqlConf.Password, Conf.MysqlConf.Host, Conf.MysqlConf.Port,
		Conf.MysqlConf.DBName)
	confdb, err := sqlx.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	model.Init(confdb)
	configs := []*schema.MysqlConfig{}
	for _, config := range Conf.MysqlInstances {
		myconf := schema.MysqlConfig{
			Name:     config.Name,
			Host:     config.Host,
			Port:     config.Port,
			Username: config.Username,
			Password: config.Password,
			Exclude:  config.Exclude,
			Interval: time.Second * 10,
		}
		configs = append(configs, &myconf)
	}
	columnManager, err = schema.NewColumnManager(configs)

	go StartServer()
	HandleSignal()
}

// HandleSignal fetch signal from chan then do exit or reload.
func HandleSignal() {
	// Block until a signal is received.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGSTOP)
	for {
		s := <-c
		log.Infof("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			time.Sleep(1 * time.Second)
			return
		case syscall.SIGHUP:
			// TODO reload
			//return
		default:
			return
		}
	}
}
