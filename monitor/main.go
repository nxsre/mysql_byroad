package main

import (
	"fmt"
	"mysql_byroad/model"
	"mysql_byroad/mysql_schema"
	"mysql_byroad/nsq"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/jmoiron/sqlx"
	"github.com/samuel/go-zookeeper/zk"
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
	if err != nil {
		log.Errorf("new column manager error: %s", err.Error())
	}
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

type empty struct{}

// 得到任务订阅的字段信息对应的所有的topic
func getTopics(task *model.Task) []string {
	topics := make([]string, 0, 10)
	set := make(map[string]empty)
	allTopics, err := getAllTopics()
	if err != nil {
		log.Errorf("get topics: %s", err.Error())
		return topics
	}
	for _, field := range task.Fields {
		matched := getMatchedTopics(allTopics, field)
		for _, topic := range matched {
			set[topic] = empty{}
		}
	}
	for topic, _ := range set {
		topics = append(topics, topic)
	}
	return topics
}

// 从zookeeper中得到所有的topic
func getAllTopics() ([]string, error) {
	conn, _, err := zk.Connect(Conf.ZkAddrs, time.Second*10)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	children, _, err := conn.Children(Conf.ZKPrefix + "/brokers/topics")
	if err != nil {
		return nil, err
	}
	log.Debugf("get all topics: %+v", children)
	return children, nil
}

// 匹配用户订阅的字段信息和topic，返回匹配上的topic
func getMatchedTopics(topics []string, field *model.NotifyField) []string {
	matched := make([]string, 0, 10)
	for _, topic := range topics {
		s := strings.SplitN(topic, "___", 2)
		if len(s) != 2 {
			continue
		}
		schema := s[0]
		table := s[1]
		if isMatch(field.Schema, schema) && isMatch(field.Table, table) {
			matched = append(matched, topic)
		}
	}
	return matched
}

/*
判断s2是否符合s1的规则
*/
func isMatch(s1, s2 string) bool {
	if s1 == s2 {
		return true
	}
	reg, err := regexp.Compile("^" + s1 + "$")
	if err != nil {
		return false
	}
	return reg.MatchString(s2)
}
