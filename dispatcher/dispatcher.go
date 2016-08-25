package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
)

type Dispatcher struct {
	rpcServer            *RPCServer
	Config               *Config
	pingTicker           *time.Ticker
	kafkaConsumerManager *KafkaConsumerManager
	taskManager          *TaskManager
}

func NewDispatcher(config *Config) *Dispatcher {
	dispatcher := &Dispatcher{
		Config: config,
	}
	rpcServerSchema := fmt.Sprintf("%s:%d", config.RPCServerConf.Host, config.RPCServerConf.Port)
	rpcClientSchema := fmt.Sprintf("%s:%d", config.MonitorConf.Host, config.MonitorConf.RpcPort)
	rpcServer := NewRPCServer(rpcServerSchema, config.RPCServerConf.Desc)
	dispatcher.rpcServer = rpcServer
	kafkaConsumerManager := NewKafkaConsumerManager(config.KafkaConf)
	dispatcher.kafkaConsumerManager = kafkaConsumerManager
	taskManager := NewTaskManager(rpcClientSchema)
	dispatcher.taskManager = taskManager
	return dispatcher
}

func (d *Dispatcher) Start() {
	rpcClientSchema := fmt.Sprintf("%s:%d", d.Config.MonitorConf.Host, d.Config.MonitorConf.RpcPort)
	tasks, err := d.taskManager.InitTasks()
	if err != nil {
		log.Errorf("init tasks error: %s", err.Error())
	}
	d.kafkaConsumerManager.InitConsumers(tasks)
	handler, err := NewKafkaEventHandler(d.Config.NSQConf, d.taskManager)
	if err != nil {
		log.Errorf("new kafka event handler error: %s", err.Error())
	}
	d.kafkaConsumerManager.AddHandler(handler)
	d.rpcServer.initServer(d.taskManager, handler.BinlogStatistics, d.kafkaConsumerManager)
	d.rpcServer.startRpcServer()
	rpcClient := NewRPCClient(rpcClientSchema)
	rpcClient.RegisterClient(d.rpcServer.getSchema(), d.rpcServer.desc)
	d.pingTicker = rpcClient.PingLoop(d.rpcServer.getSchema(), d.rpcServer.desc, d.Config.RPCPingInterval.Duration)
}

func (d *Dispatcher) Stop() {
	d.pingTicker.Stop()
	rpcClientSchema := fmt.Sprintf("%s:%d", d.Config.MonitorConf.Host, d.Config.MonitorConf.RpcPort)
	rpcClient := NewRPCClient(rpcClientSchema)
	rpcClient.DeregisterClient(d.rpcServer.getSchema(), d.rpcServer.desc)
	d.kafkaConsumerManager.StopConsumers()
}

// HandleSignal fetch signal from chan then do exit or reload.
func (d *Dispatcher) HandleSignal() {
	// Block until a signal is received.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGSTOP)
	for {
		s := <-c
		log.Infof("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			d.Stop()
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
