package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"io/ioutil"

	"time"

	"math/rand"

	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
)

type QueueManager struct {
	lookupdAddrs []string
	nsqdAddrs    []string
	producers    []*nsq.Producer
	config       *nsq.Config
}

type node struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	HTTPPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
}

func NewQueueManager(lookupAddrs []string) (*QueueManager, error) {
	qm := &QueueManager{
		lookupdAddrs: lookupAddrs,
		config:       nsq.NewConfig(),
	}
	qm.nsqdAddrs = getNodesInfo(lookupAddrs)
	qm.initProducers()
	go qm.updateProducer()
	return qm, nil
}

func getNodesInfo(lookupAddrs []string) []string {
	nodesInfo := make([]string, 0, 10)
	for _, addr := range lookupAddrs {
		endpoint := fmt.Sprintf("http://%s/nodes", addr)
		resp, err := http.Get(endpoint)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("nsqlookupd read error: %s", err.Error())
			continue
		}
		resp.Body.Close()
		var u struct {
			StatusCode int64           `json:"status_code"`
			StatusText string          `json:"status_txt"`
			Data       json.RawMessage `json:"data"`
		}

		err = json.Unmarshal(body, &u)
		if err != nil {
			log.Errorf("json unmarshal error1: %s", err.Error())
			continue
		}
		if u.StatusCode != 200 {
			log.Errorf("api status code: %d, %s", u.StatusCode, u.StatusText)
			continue
		}
		var v struct {
			Producers []*node
		}
		err = json.Unmarshal(u.Data, &v)
		if err != nil {
			log.Errorf("json unmarshal error2: %s", err.Error())
			continue
		}
		for _, pro := range v.Producers {
			log.Debugf("producers %+v", pro)
			nodesInfo = append(nodesInfo, fmt.Sprintf("%s:%d", pro.Hostname, pro.TCPPort))
		}
	}
	return nodesInfo
}

func (qm *QueueManager) initProducers() {
	producers := qm.getProducers()
	qm.producers = producers
}

func (qm *QueueManager) getProducers() []*nsq.Producer {
	producers := make([]*nsq.Producer, 0, 10)
	for _, node := range qm.nsqdAddrs {
		pro, err := nsq.NewProducer(node, qm.config)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		err = pro.Ping()
		if err == nil {
			producers = append(producers, pro)
		}
	}
	return producers
}

func (qm *QueueManager) updateProducer() {
	ticker := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-ticker.C:
			/*
				idx := make([]int,0,10)
				for index, pro := range qm.producers{
					if err := pro.Ping(); err != nil {
						log.Warn("nsqd update error ", err.Error())
						idx = append(idx, index)
					}
				}
				for _, id := range idx{


			}*/
			//log.Debugf("update producers: %d", len(qm.producers))
		}
	}
}

func (qm *QueueManager) GetProducer() (*nsq.Producer, error) {
	if len(qm.producers) != 0 {
		i := rand.Intn(len(qm.producers))
		return qm.producers[i], nil
	}
	return nil, fmt.Errorf("no nsqd server avaiable")
}

func (qm *QueueManager) Enqueue(name string, evt interface{}) {
	log.Debug("nsq publish ", name)
	p, err := qm.GetProducer()
	if err == nil {
		evtMsg, err := json.Marshal(evt)
		if err != nil {
			log.Error(err.Error())
		}
		err = p.Publish(name, evtMsg)
		if err != nil {
			log.Error(err.Error())
		}
	} else {
		log.Error("nsq enqueue error: ", err.Error())
	}
}
