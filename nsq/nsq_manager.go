package nsqm

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
)

type NSQManager struct {
	lookupdAddrs []string
	nsqdNodes    []*node
	producers    map[string]*nsq.Producer
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

func NewNSQManager(lookupAddrs []string) (*NSQManager, error) {
	qm := &NSQManager{
		lookupdAddrs: lookupAddrs,
		config:       nsq.NewConfig(),
	}
	qm.nsqdNodes = getNodesInfo(lookupAddrs)
	return qm, nil
}

func (qm *NSQManager) ProducerLookup() {
	qm.initProducers()
	go qm.updateProducer()
}

func getNodesInfo(lookupAddrs []string) []*node {
	nodesInfo := make([]*node, 0, 10)
	for _, addr := range lookupAddrs {
		endpoint := fmt.Sprintf("http://%s/nodes", addr)
		resp, err := http.Get(endpoint)
		if err != nil {
			log.Error("nsq get node info: ", err.Error())
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
		nodesInfo = append(nodesInfo, v.Producers...)
		for _, pro := range nodesInfo {
			log.Debugf("get node info %+v", pro)
		}
		/*	for _, pro := range v.Producers {
			log.Debugf("get producers %+v", pro)
			nodesInfo = append(nodesInfo, fmt.Sprintf("%s:%d", pro.Hostname, pro.TCPPort))
		}*/
	}
	return nodesInfo
}

func (qm *NSQManager) initProducers() {
	producers := qm.getProducers()
	qm.producers = producers
}

func (qm *NSQManager) getProducers() map[string]*nsq.Producer {
	producers := make(map[string]*nsq.Producer, 10)
	for _, node := range qm.nsqdNodes {
		nodeaddr := fmt.Sprintf("%s:%d", node.Hostname, node.TCPPort)
		pro, err := nsq.NewProducer(nodeaddr, qm.config)
		if err != nil {
			log.Error("nsq get producer: ", err.Error())
			continue
		}
		err = pro.Ping()
		if err != nil {
			log.Error("nsq ping error: ", err.Error())
			continue
		}
		producers[nodeaddr] = pro
	}
	return producers
}

func (qm *NSQManager) updateProducer() {
	ticker := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-ticker.C:
			nsqnodes := getNodesInfo(qm.lookupdAddrs)
			for _, n := range nsqnodes {
				nsqaddr := fmt.Sprintf("%s:%d", n.Hostname, n.TCPPort)
				if pro, ok := qm.producers[nsqaddr]; ok {
					if err := pro.Ping(); err != nil {
						log.Error("nsqd ping error: ", err.Error())
						delete(qm.producers, nsqaddr)
					}
				} else {
					pro, err := nsq.NewProducer(nsqaddr, qm.config)
					if err != nil {
						log.Error("nsq new producer error: ", err.Error())
					} else {
						if err := pro.Ping(); err != nil {
							log.Error("nsq ping error: ", err.Error())
						} else {
							qm.producers[nsqaddr] = pro
						}
					}
				}
			}
			qm.nsqdNodes = nsqnodes
		}
	}
}

func (qm *NSQManager) GetProducer() (*nsq.Producer, error) {
	if len(qm.producers) != 0 {
		i := rand.Intn(len(qm.nsqdNodes))
		log.Debugf("nsq nodes lenght: %d, rand: %d", len(qm.nsqdNodes), i)
		n := qm.nsqdNodes[i]
		addr := fmt.Sprintf("%s:%d", n.Hostname, n.TCPPort)
		if pro, ok := qm.producers[addr]; ok {
			log.Debug("get producer ", addr)
			return pro, nil
		} else {
			return qm.GetProducer()
		}
	}
	return nil, fmt.Errorf("no nsqd server avaiable")
}

func (qm *NSQManager) Enqueue(name string, evt interface{}) {
	log.Info("nsq publish ", name)
	p, err := qm.GetProducer()
	if err == nil {
		evtMsg, err := json.Marshal(evt)
		if err != nil {
			log.Error("json marshal: ", err.Error())
		}
		err = p.Publish(name, evtMsg)
		if err != nil {
			log.Error("nsq publish: ", err.Error())
		}
	} else {
		log.Error("nsq enqueue error: ", err.Error())
	}
}

func (qm *NSQManager) NewNSQConsumer(topic, channel string, concurrency int) (*nsq.Consumer, error) {
	log.Infof("new consumer %s/%s", topic, channel)
	config := nsq.NewConfig()
	c, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		log.Error("nsq new comsumer: ", err.Error())
		return c, err
	}
	err = c.ConnectToNSQLookupds(qm.lookupdAddrs)
	if err != nil {
		return c, err
		log.Error("nsq connect to nsq lookupds: ", err.Error())
	}
	return c, nil
}

func (qm *NSQManager) GetStats() []*NodeStats {
	stats := make([]*NodeStats, 0, 10)
	for _, n := range qm.nsqdNodes {
		addr := fmt.Sprintf("%s:%d", n.Hostname, n.HTTPPort)
		s, err := getNodeStats(addr)
		if err != nil {
			log.Error("get node stats error: ", err.Error())
			continue
		} else {
			ns := &NodeStats{
				Node:  n,
				Stats: s,
			}
			stats = append(stats, ns)
		}
	}
	return stats
}

func getNodeStats(addr string) (*Stats, error) {
	url := fmt.Sprintf("http://%s/stats?format=json", addr)

	req, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer req.Body.Close()

	var s *stats
	err = json.NewDecoder(req.Body).Decode(&s)
	return s.Data, err
}

func (qm *NSQManager) GetTopicStats(topicname string) []*TopicStats {
	allStats := qm.GetStats()
	topicStats := make([]*TopicStats, 0, 10)
	for _, ns := range allStats {
		for _, topic := range ns.Stats.Topics {
			if topic.Name == topicname {
				ts := &TopicStats{
					Node:  ns.Node,
					Topic: topic,
				}
				topicStats = append(topicStats, ts)
			}
		}
	}
	return topicStats
}
