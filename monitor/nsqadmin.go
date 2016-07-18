package main

import (
	"encoding/json"
	"fmt"
	"mysql_byroad/model"
	"net/http"

	"io/ioutil"

	log "github.com/Sirupsen/logrus"
)

type NSQAdmin struct {
	nsqlookupdAddr string
	httpClient     *http.Client
}

// stats with envelope.
type stats struct {
	Data *Stats
}

// Stats.
type Stats struct {
	Version string   `json:"version"`
	Health  string   `json:"health"`
	Topics  []*Topic `json:"topics"`
}

// Topic stats.
type Topic struct {
	Name          string     `json:"topic_name"`
	InFlightCount int64      `json:"in_flight_count"`
	DeferredCount int64      `json:"deferred_count"`
	MessageCount  int64      `json:"message_count"`
	RequeueCount  int64      `json:"requeue_count"`
	TimeoutCount  int64      `json:"timeout_count"`
	BackendDepth  int64      `json:"backend_depth"`
	Depth         int64      `json:"depth"`
	Paused        bool       `json:"paused"`
	Channels      []*Channel `json:"channels"`
}

// Channel stats.
type Channel struct {
	Name          string `json:"channel_name"`
	InFlightCount int64  `json:"in_flight_count"`
	DeferredCount int64  `json:"deferred_count"`
	MessageCount  int64  `json:"message_count"`
	RequeueCount  int64  `json:"requeue_count"`
	TimeoutCount  int64  `json:"timeout_count"`
	BackendDepth  int64  `json:"backend_depth"`
	Depth         int64  `json:"depth"`
	Paused        bool   `json:"paused"`
}

type TopicStats struct {
	Node         string          `json:"node"`
	Hostname     string          `json:"hostname"`
	TopicName    string          `json:"topic_name"`
	Depth        int64           `json:"depth"`
	MemoryDepth  int64           `json:"memory_depth"`
	BackendDepth int64           `json:"backend_depth"`
	MessageCount int64           `json:"message_count"`
	NodeStats    json.RawMessage `json:"nodes"`
	Channels     json.RawMessage `json:"channels"`
	Paused       bool            `json:"paused"`
	Message      string          `json:"message"`

	E2eProcessingLatency json.RawMessage `json:"e2e_processing_latency"`
}

func NewNSQAdmin(addr string) *NSQAdmin {
	admin := &NSQAdmin{
		nsqlookupdAddr: addr,
		httpClient:     &http.Client{},
	}
	return admin
}

func (admin *NSQAdmin) GetTaskQueueLength(task *model.Task) (int64, error) {
	endpoint := fmt.Sprintf("http://%s/api/topics/%s", admin.nsqlookupdAddr, task.Name)
	resp, err := admin.httpClient.Get(endpoint)
	if err != nil {
		log.Error("get task queue length error: ", err.Error())
		return 0, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	ts := TopicStats{}
	err = json.Unmarshal(body, &ts)
	if err != nil {
		return 0, err
	}
	return ts.Depth, nil
}
