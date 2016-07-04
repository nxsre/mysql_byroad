package slave

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"mysql_byroad/model"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var httpClient *http.Client

func NewHttpClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: configer.GetInt("httpclient", "MaxIdleConnsPerHost"),
		},
	}
}

/*
消息推送协程，读取推送消息队列，将消息发送到对应任务的apiurl中
如果发送失败，将消息放入重推队列中
*/
func notifyRoutine(name string) {
	ele := queueManager.Dequeue(name)
	if ele == nil {
		time.Sleep(time.Millisecond * 10) //10 Millisecond?
		return
	}
	evt := new(model.NotifyEvent)
	err := json.Unmarshal(ele.([]byte), evt)
	if err != nil {
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
	}
	ret, _ := sendMessage(evt)
	totalStatistic.IncSendMessageCount()
	taskStatistics.IncSendMessageCount(evt.TaskID)
	if !isSuccessSend(ret) {
		task := GetTask(evt.TaskID)
		if task == nil {
			return
		}
		queueManager.Enqueue(genTaskReQueueName(task), evt)
	} else {
		totalStatistic.IncSendSuccessCount()
		taskStatistics.IncSendSuccessCount(evt.TaskID)
	}
}

/*
消息重推协程，读取重推消息队列，将消息发送到对应任务的apiurl中，可以设置重推的时间间隔
如果发送失败，将消息放入重推队列中
如果发送失败超过一定次数，记录日志，丢弃该消息
*/
func notifyRetryRoutine(name string) {
	ele := queueManager.Dequeue(name)
	if ele == nil {
		time.Sleep(time.Millisecond * 10)
		return
	}
	evt := new(model.NotifyEvent)
	err := json.Unmarshal(ele.([]byte), evt)
	if err != nil {
		sysLogger.Log(err.Error())
		owl.LogThisException(err.Error())
	}
	if !isSend(evt) {
		queueManager.Enqueue(name, evt)
		return
	}
	evt.RetryCount++
	ret, err := sendMessage(evt)
	totalStatistic.IncReSendMessageCount()
	taskStatistics.IncReSendMessageCount(evt.TaskID)
	if !isSuccessSend(ret) {
		task := GetTask(evt.TaskID)
		if task == nil {
			return
		}
		if evt.RetryCount >= task.RetryCount {
			totalStatistic.IncSendFailedCount()
			taskStatistics.IncSendFailedCount(evt.TaskID)
			if err != nil {
				logNotifyMessage(evt, err)
			} else {
				logNotifyMessage(evt, errors.New(ret))
			}
			return
		}
		queueManager.Enqueue(name, evt)
	} else {
		totalStatistic.IncSendSuccessCount()
		taskStatistics.IncSendSuccessCount(evt.TaskID)
	}
}

/*
比较消息上一次的推送时间，判断消息是否推送
*/

func isSend(e *model.NotifyEvent) bool {
	dur := time.Now().Sub(e.LastSendTime)
	task := GetTask(e.TaskID)
	if task == nil {
		return true
	}
	return dur > time.Duration(task.ReSendTime)*time.Millisecond
}

/*
发送消息
*/
func sendMessage(evt *model.NotifyEvent) (string, error) {
	task := GetTask(evt.TaskID)
	if task == nil {
		return "success", nil
	}
	evt.LastSendTime = time.Now()
	msg, _ := json.Marshal(evt)
	timeout := time.Millisecond * time.Duration(task.Timeout)
	if task.PackProtocal == model.PackProtocalEventCenter {
		idStr := strconv.FormatInt(task.ID, 10)
		retryCountStr := strconv.Itoa(evt.RetryCount)
		pushurl := task.Apiurl + "?" + url.Values{"jobid": {idStr}, "retry_times": {retryCountStr}}.Encode()
		body := url.Values{"message": {string(msg)}}
		resp, err := httpClient.PostForm(pushurl, body)
		if err != nil {
			return "fail", err
		}
		defer resp.Body.Close()
		retStat, err := ioutil.ReadAll(resp.Body)
		return string(retStat), err
	} else {
		body := bytes.NewBuffer(msg)
		httpClient.Timeout = timeout
		resp, err := httpClient.Post(task.Apiurl, "application/json", body)
		if err != nil {
			return "fail", err
		}
		defer resp.Body.Close()
		retStat, err := ioutil.ReadAll(resp.Body)
		return string(retStat), err
	}
	return "success", nil
}

/*
记录消息推送失败的日志
*/
func logNotifyMessage(msg *model.NotifyEvent, reason error) {
	eventLogger.Log(msg, reason)
}

type SendResp struct {
	Status int `json:"status"`
}

func isSuccessSend(msg string) bool {
	if msg == "success" {
		return true
	} else {
		var sendResp SendResp
		if json.Unmarshal([]byte(msg), &sendResp) == nil {
			if sendResp.Status == 1 {
				return true
			}
		}
		return false
	}
}
