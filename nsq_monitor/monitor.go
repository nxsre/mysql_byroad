package main

import (
	"fmt"
	"mysql_byroad/model"
	"mysql_byroad/notice"
	"mysql_byroad/nsq"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

type MonitorConfig struct {
	MaxChannelDepth int64
	LookupInterval  time.Duration
	PhoneNumbers    []string
	Emails          []string
}

type NSQMonitor struct {
	nsqdAddrs    []string
	lookupdAddrs []string
	config       *MonitorConfig
}

func initAlert(config *notice.Config) {
	notice.Init(config)
}

func NewNSQMonitor(nsqdAddrs, lookupdAddrs []string, config *MonitorConfig, noticeConfig *notice.Config) *NSQMonitor {
	monitor := &NSQMonitor{
		nsqdAddrs:    nsqdAddrs,
		lookupdAddrs: lookupdAddrs,
		config:       config,
	}
	initAlert(noticeConfig)
	return monitor
}

func (this *NSQMonitor) RunNSQDMonitor() {
	go this.nsqdMonitorLoop()
}

func (this *NSQMonitor) nsqdCheck() {
	nodes, err := nsqm.GetNodesInfo(this.lookupdAddrs)
	if err != nil {
		log.Errorf("get nsqd node info error: %s", err.Error())
		return
	}
	for _, node := range nodes {
		addr := fmt.Sprintf("%s:%d", node.BroadcastAddress, node.HTTPPort)
		this.checkNode(addr)
	}
	for _, nsqdAddr := range this.nsqdAddrs {
		this.checkNode(nsqdAddr)
	}
}

func (this *NSQMonitor) nsqdMonitorLoop() {
	ticker := time.NewTicker(this.config.LookupInterval)
	for {
		select {
		case <-ticker.C:
			this.nsqdCheck()
		}
	}
}

func (this *NSQMonitor) checkNode(nsqdAddr string) {
	stats, err := nsqm.GetNodeStats(nsqdAddr)
	if err != nil {
		log.Errorf("get node %s error: %s", nsqdAddr, err.Error())
		this.sendAlert(this.config.PhoneNumbers, this.config.Emails, "【旁路系统】get nsqd node %s error: %s", nsqdAddr, err.Error())
		return
	}
	if stats.Health != "OK" {
		log.Errorf("%s health status: %s", nsqdAddr, stats.Health)
		this.sendAlert(this.config.PhoneNumbers, this.config.Emails, "【旁路系统】 nsqd node %s health status: %s", nsqdAddr, stats.Health)
	}
	for _, topic := range stats.Topics {
		for _, channel := range topic.Channels {
			log.Infof("topic: %s, channel: %s, depth: %d", topic.Name, channel.Name, channel.Depth)
			if channel.Depth+channel.BackendDepth > this.config.MaxChannelDepth {
				task, err := model.GetTaskByName(channel.Name)
				if err != nil {
					log.Errorf("get task error: %s", err.Error())
					continue
				}
				log.Infof("Host: %s\nTopic: %s\nChannel: %s\nDepth: %d", nsqdAddr, topic.Name, channel.Name, channel.Depth)
				if task.Alert == 1 {
					var phoneNumbers, emails []string
					if task.PhoneNumbers != "" {
						phoneNumbers = strings.Split(task.PhoneNumbers, ";")
					}
					if task.Emails != "" {
						emails = strings.Split(task.Emails, ";")
					}
					this.sendAlert(phoneNumbers, emails, "【旁路系统】消息队列长度报警\nTime: %s\nHost: %s\nTopic: %s\nChannel: %s\nDepth: %d", time.Now().String(), nsqdAddr, topic.Name, channel.Name, channel.Depth)
				}
			}
		}
	}
}

func (this *NSQMonitor) sendAlert(phoneNumbers, emails []string, format string, a ...interface{}) {
	content := fmt.Sprintf(format, a...)
	for _, number := range phoneNumbers {
		number = strings.TrimSpace(number)
		ret, err := notice.SendSms(number, content)
		if err != nil {
			log.Infof("send sms %s: %s,\nret: %s error: %s", number, content, ret, err.Error())
		} else {
			log.Infof("send sms %s: %s,\nret: %s", number, content, ret)
		}
	}
	for _, email := range emails {
		email = strings.TrimSpace(email)
		ret, err := notice.SendEmail(email, "旁路系统", content)
		if err != nil {
			log.Infof("send sms %s: %s,\nret: %s error: %s", email, content, ret, err.Error())
		} else {
			log.Infof("send email %s: %s,\nret: %s", email, content, ret)
		}
	}
}
