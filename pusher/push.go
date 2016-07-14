package main

import (
	"bytes"
	"encoding/json"
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
			MaxIdleConnsPerHost: 1000,
		},
	}
}

func init() {
	httpClient = NewHttpClient()
}

/*
发送消息
*/
func sendMessage(evt *model.NotifyEvent) (string, error) {
	task := taskManager.GetTask(evt.TaskID)
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
