package main

import (
	"fmt"
	"mysql_byroad/model"
	"mysql_byroad/notice"
	"strings"
	"sync"

	"time"

	log "github.com/Sirupsen/logrus"
)

var errorStaticMap *ErrorStaticMap = NewErrorStaticMap()

func InitAlert(config *AlertConfig) {
	c := notice.Config{
		User:      config.User,
		Password:  config.Password,
		SmsAddr:   config.SmsAddr,
		EmailAddr: config.EmailAddr,
	}
	notice.Init(&c)
}

type ErrorStaticMap struct {
	static         map[int64]int
	lock           sync.RWMutex
	ticker         *time.Ticker
	stopStaticChan chan bool
}

func NewErrorStaticMap() *ErrorStaticMap {
	esmap := &ErrorStaticMap{
		static:         make(map[int64]int),
		ticker:         time.NewTicker(time.Minute),
		stopStaticChan: make(chan bool, 1),
	}
	go func(ticker *time.Ticker) {
		for {
			select {
			case <-ticker.C:
				esmap.check()
				esmap.reset()
			case <-esmap.stopStaticChan:
				return
			}
		}
	}(esmap.ticker)
	return esmap
}

func (this *ErrorStaticMap) check() {
	this.lock.RLock()
	for taskid, count := range this.static {
		if count > 3 {
			task := taskManager.GetTask(taskid)
			if task == nil {
				continue
			}
			sendTimerAlert(task, count)
		}
	}
	this.lock.RUnlock()
}

func (this *ErrorStaticMap) reset() {
	this.lock.Lock()
	for key, _ := range this.static {
		this.static[key] = 0
	}
	this.lock.Unlock()
}

func (this *ErrorStaticMap) StopStatic() {
	this.stopStaticChan <- true
	this.ticker.Stop()
}

func (this *ErrorStaticMap) Get(task *model.Task) (int, bool) {
	this.lock.RLock()
	count, ok := this.static[task.ID]
	this.lock.RUnlock()
	return count, ok
}

func (this *ErrorStaticMap) Set(task *model.Task, count int) {
	this.lock.Lock()
	this.static[task.ID] = count
	this.lock.Unlock()
}

func (this *ErrorStaticMap) Inc(task *model.Task) {
	count, _ := this.Get(task)
	this.Set(task, count+1)
}

func handleAlert(evt *model.NotifyEvent, reason string) {
	task := taskManager.GetTask(evt.TaskID)
	if task == nil || task.Alert == 0 {
		return
	}
	errorStaticMap.Inc(task)
	retryThreshold := task.RetryCount * 7 / 10
	if evt.RetryCount >= retryThreshold {
		sendFailAlert(task, evt, reason)
	}
}

func sendFailAlert(task *model.Task, evt *model.NotifyEvent, reason string) {
	content := fmt.Sprintf("%s: 旁路系统：%s 任务推送失败，次数: %d, 原因: %s", time.Now().String(), task.Name, evt.RetryCount, reason)
	sendAlert(task, content)
}

func sendTimerAlert(task *model.Task, count int) {
	content := fmt.Sprintf("%s: 旁路系统：%s 任务一分钟推送失败次数: %d", time.Now().String(), task.Name, count)
	sendAlert(task, content)
}

func sendAlert(task *model.Task, content string) {
	var numbers, emails []string
	if task.PhoneNumbers != "" {
		numbers = strings.Split(task.PhoneNumbers, ";")
	}
	if task.Emails != "" {
		emails = strings.Split(task.Emails, ";")
	}
	for _, number := range numbers {
		number = strings.TrimSpace(number)
		log.Infof("send sms %s: %s", number, content)
		ret, err := notice.SendSms(number, content)
		if err != nil {
			log.Errorf("send sms error %s:%s", ret, err.Error())
		}
	}
	for _, e := range emails {
		e = strings.TrimSpace(e)
		log.Infof("send email %s: %s", e, content)
		ret, err := notice.SendEmail(e, "旁路系统", content)
		if err != nil {
			log.Errorf("send email error %s:%s", ret, err.Error())
		}
	}
}
