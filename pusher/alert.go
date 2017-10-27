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

var errorStaticMap *ErrorStaticMap
var alertMap *AlertMap

func InitAlert(config *AlertConfig) {
	c := notice.Config{
		User:      config.User,
		Password:  config.Password,
		SmsAddr:   config.SmsAddr,
		EmailAddr: config.EmailAddr,
	}
	notice.Init(&c)
	errorStaticMap = NewErrorStaticMap()
	alertMap = NewAlertMap()
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
		ticker:         time.NewTicker(Conf.AlertConfig.Period.Duration),
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
		if count > Conf.AlertConfig.MaxCount {
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

type AlertMap struct {
	m    map[*model.Task]string
	lock sync.RWMutex
}

type AlertMapEntry struct {
	key   *model.Task
	value string
}

func NewAlertMap() *AlertMap {
	am := &AlertMap{
		m: make(map[*model.Task]string),
	}
	go func() {
		ticker := time.NewTicker(Conf.AlertConfig.Interval.Duration)
		for {
			select {
			case <-ticker.C:
				for entry := range alertMap.Iter() {
					task, content := entry.key, entry.value
					sendAlertNow(task, content)
				}
				alertMap.Empty()
			}
		}
	}()
	return am
}

func (amap *AlertMap) Put(key *model.Task, value string) {
	amap.lock.Lock()
	defer amap.lock.Unlock()
	amap.m[key] = value
}

func (amap *AlertMap) Get(key *model.Task) string {
	amap.lock.RLock()
	defer amap.lock.RUnlock()
	return amap.m[key]
}

func (amap *AlertMap) Len() int {
	amap.lock.RLock()
	defer amap.lock.RUnlock()
	return len(amap.m)
}

func (amap *AlertMap) Iter() chan *AlertMapEntry {
	c := make(chan *AlertMapEntry)
	go func() {
		amap.lock.RLock()
		for k, v := range amap.m {
			entry := &AlertMapEntry{
				key:   k,
				value: v,
			}
			c <- entry
		}
		amap.lock.RUnlock()
		close(c)
	}()
	return c
}

func (amap *AlertMap) IterBuffered() chan *AlertMapEntry {
	c := make(chan *AlertMapEntry, amap.Len())
	go func() {
		amap.lock.RLock()
		for k, v := range amap.m {
			entry := &AlertMapEntry{
				key:   k,
				value: v,
			}
			c <- entry
		}
		amap.lock.RUnlock()
		close(c)
	}()
	return c
}

func (amap *AlertMap) Empty() {
	amap.lock.Lock()
	defer amap.lock.Unlock()
	for k, _ := range amap.m {
		delete(amap.m, k)
	}
}

func (amap *AlertMap) Delete(k *model.Task) {
	amap.lock.Lock()
	defer amap.lock.Unlock()
	delete(amap.m, k)
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

/*
旁路系统\n时间：2016-10-31 11:05:10.936761235 +0800 CST\n任务：test_user2\n消息：1m0s内推送失败次数: 5
旁路系统\n时间：%s\n任务：%s\n消息：%s内推送失败次数:%d
旁路系统\n时间：2016-10-31 11:04:46.696365016 +0800 CST\n任务：test_user2\n消息：任务推送失败2次, 原因: Post http://localhost:8091/get: dial tcp 127.0.0.1:8091: getsockopt: connection refused
旁路系统\n时间：%s\n任务：%s\n消息：任务推送失败%d次, 原因: %s
*/
func sendFailAlert(task *model.Task, evt *model.NotifyEvent, reason string) {
	content := fmt.Sprintf("旁路系统\n时间：%s\n任务：%s\n消息：任务推送失败%d次, 原因: %s", time.Now().String(), task.Name, evt.RetryCount, reason)
	sendAlert(task, content)
}

func sendTimerAlert(task *model.Task, count int) {
	content := fmt.Sprintf("旁路系统\n时间：%s\n任务：%s\n消息：%s内推送失败次数:%d", time.Now().String(), task.Name, Conf.AlertConfig.Period.String(), count)
	sendAlertNow(task, content)
}

func sendAlert(task *model.Task, content string) {
	alertMap.Put(task, content)
}

func sendAlertNow(task *model.Task, content string) {
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
