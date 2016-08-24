package nsqm

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/nsqio/go-nsq"
)

var (
	nsqManager *NSQManager
	once       sync.Once
)

type ErrList []error

func (l ErrList) Error() string {
	var es []string
	for _, e := range l {
		es = append(es, e.Error())
	}
	return strings.Join(es, "\n")
}

func (l ErrList) Errors() []error {
	return l
}

type NSQManager struct {
	lookupdAddrs []string
	nsqaddrs     []string
	nsqdNodes    []*Node
	producers    map[string]*nsq.Producer
	config       *nsq.Config
	sync.RWMutex
}

func GetManager(lookupAddrs []string, nsqaddrs []string, config *nsq.Config) (*NSQManager, error) {
	var err error
	once.Do(func() {
		nsqManager, err = NewNSQManager(lookupAddrs, nsqaddrs, config)
		nsqManager.InitProducers()
		nsqManager.ProducerUpdateLoop()
	})
	return nsqManager, err
}

func NewNSQManager(lookupAddrs []string, nsqaddrs []string, config *nsq.Config) (*NSQManager, error) {
	if config == nil {
		config = nsq.NewConfig()
	}
	qm := &NSQManager{
		lookupdAddrs: lookupAddrs,
		nsqaddrs:     nsqaddrs,
		config:       config,
	}
	var err error
	qm.nsqdNodes, err = getNodesInfo(lookupAddrs)
	return qm, err
}

func (qm *NSQManager) InitProducers() {
	producers, _ := qm.getProducers()
	qm.Lock()
	qm.producers = producers
	qm.Unlock()
}

func (qm *NSQManager) ProducerUpdateLoop() {
	go qm.updateProducer()
}

func (qm *NSQManager) NodeInfoUpdateLoop() {
	go func() {
		ticker := time.NewTicker(time.Second * 60)
		for {
			select {
			case <-ticker.C:
				nodes, _ := getNodesInfo(qm.lookupdAddrs)
				qm.Lock()
				qm.nsqdNodes = nodes
				qm.Unlock()
			}
		}
	}()
}

func (qm *NSQManager) GetNodesInfo() ([]*Node, error) {
	nodes, err := getNodesInfo(qm.lookupdAddrs)
	qm.Lock()
	qm.nsqdNodes = nodes
	qm.Unlock()
	return nodes, err
}

// 通过nsqlookupd的nodes接口获取所有nsqd节点信息
func getNodesInfo(lookupAddrs []string) ([]*Node, error) {
	var errs []error
	nodesInfo := make([]*Node, 0, 10)
	for _, addr := range lookupAddrs {
		endpoint := fmt.Sprintf("http://%s/nodes", addr)
		resp, err := http.Get(endpoint)
		if err != nil {
			log.Error("nsq get node info: ", err.Error())
			errs = append(errs, err)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errs = append(errs, err)
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
			errs = append(errs, err)
			log.Errorf("json unmarshal error1: %s", err.Error())
			continue
		}
		if u.StatusCode != 200 {
			errs = append(errs, err)
			log.Errorf("api status code: %d, %s", u.StatusCode, u.StatusText)
			continue
		}
		var v struct {
			Producers []*Node
		}
		err = json.Unmarshal(u.Data, &v)
		if err != nil {
			errs = append(errs, err)
			log.Errorf("json unmarshal error2: %s", err.Error())
			continue
		}
		nodesInfo = append(nodesInfo, v.Producers...)
	}
	if len(errs) > 0 {
		return nodesInfo, ErrList(errs)
	}
	return nodesInfo, nil
}

// 通过nsqlookupd的lookup接口，获取某一topic的所有nsqd节点信息
func (qm *NSQManager) GetProducer(topicname string) ([]*Producer, error) {
	var errs []error
	proNodes := make([]*Producer, 0, 10)
	for _, lookup := range qm.lookupdAddrs {
		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", lookup, topicname)
		resp, err := http.Get(endpoint)
		if err != nil {
			errs = append(errs, err)
			log.Error("nsq get producer node info: ", err.Error())
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errs = append(errs, err)
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
			errs = append(errs, err)
			log.Errorf("json unmarshal error1: %s", err.Error())
			continue
		}
		if u.StatusCode != 200 {
			errs = append(errs, err)
			log.Errorf("topic: %s, error: api status code: %d, %s", topicname, u.StatusCode, u.StatusText)
			continue
		}
		var v struct {
			Producers []*Producer
		}
		err = json.Unmarshal(u.Data, &v)
		if err != nil {
			errs = append(errs, err)
			log.Errorf("json unmarshal error2: %s", err.Error())
			continue
		}
		proNodes = append(proNodes, v.Producers...)
	}
	if len(errs) > 0 {
		return proNodes, ErrList(errs)
	}
	return proNodes, nil
}

// 通过nsqd的stats接口获取节点的统计信息
func (qm *NSQManager) GetStats(topicname string) ([]*NodeStats, error) {
	var errs []error
	stats := make([]*NodeStats, 0, 10)
	proNodes, err := qm.GetProducer(topicname)
	if err != nil {
		return stats, err
	}
	for _, n := range proNodes {
		addr := fmt.Sprintf("%s:%d", n.BroadcastAddress, n.HTTPPort)
		s, err := getNodeStats(addr)
		if err != nil {
			errs = append(errs, err)
			log.Error("get node stats error: ", err.Error())
			continue
		} else {
			ns := &NodeStats{
				Producer: n,
				Stats:    s,
			}
			stats = append(stats, ns)
		}
	}
	if len(errs) > 0 {
		return stats, ErrList(errs)
	}
	return stats, nil
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
	allStats, _ := qm.GetStats(topicname)
	topicStats := make([]*TopicStats, 0, 10)
	for _, ns := range allStats {
		for _, topic := range ns.Stats.Topics {
			if topic.Name == topicname {
				ts := &TopicStats{
					Producer: ns.Producer,
					Topic:    topic,
				}
				topicStats = append(topicStats, ts)
			}
		}
	}
	return topicStats
}

func (qm *NSQManager) getProducers() (map[string]*nsq.Producer, error) {
	var errs []error
	producers := make(map[string]*nsq.Producer, 10)
	nsqNodes, _ := qm.GetNodesInfo()
	for _, node := range nsqNodes {
		nodeaddr := fmt.Sprintf("%s:%d", node.BroadcastAddress, node.TCPPort)
		pro, err := nsq.NewProducer(nodeaddr, qm.config)
		if err != nil {
			errs = append(errs, err)
			log.Error("nsq get producer: ", err.Error())
			continue
		}
		err = pro.Ping()
		if err != nil {
			errs = append(errs, err)
			log.Error("nsq ping error: ", err.Error())
			continue
		}
		producers[nodeaddr] = pro
	}
	for _, nodeaddr := range qm.nsqaddrs {
		pro, err := nsq.NewProducer(nodeaddr, qm.config)
		if err != nil {
			errs = append(errs, err)
			log.Error("nsq get producer: ", err.Error())
			continue
		}
		err = pro.Ping()
		if err != nil {
			errs = append(errs, err)
			log.Error("nsq ping error: ", err.Error())
			continue
		}
		producers[nodeaddr] = pro
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

func (qm *NSQManager) updateProducer() {
	ticker := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-ticker.C:
			nodesInfo, _ := qm.GetNodesInfo()
			for _, n := range nodesInfo {
				nsqaddr := fmt.Sprintf("%s:%d", n.BroadcastAddress, n.TCPPort)
				if pro, ok := qm.producers[nsqaddr]; ok {
					if err := pro.Ping(); err != nil {
						log.Error("nsqd ping error: ", err.Error())
						qm.Lock()
						delete(qm.producers, nsqaddr)
						qm.Unlock()
					}
				} else {
					pro, err := nsq.NewProducer(nsqaddr, qm.config)
					if err != nil {
						log.Error("nsq new producer error: ", err.Error())
					} else {
						if err := pro.Ping(); err != nil {
							log.Error("nsq ping error: ", err.Error())
						} else {
							qm.Lock()
							qm.producers[nsqaddr] = pro
							qm.Unlock()
						}
					}
				}
			}
		}
	}
}

// 在所有的producers中随机返回一个
func (qm *NSQManager) GetOneProducer() (*nsq.Producer, error) {
	var addr string
	if len(qm.nsqaddrs) != 0 {
		i := rand.Intn(len(qm.nsqaddrs))
		addr = qm.nsqaddrs[i]
	} else if len(qm.nsqdNodes) != 0 {
		i := rand.Intn(len(qm.nsqdNodes))
		log.Debugf("nsq nodes lenght: %d, rand: %d", len(qm.nsqdNodes), i)
		n := qm.nsqdNodes[i]
		addr = fmt.Sprintf("%s:%d", n.BroadcastAddress, n.TCPPort)
	}
	if pro, ok := qm.producers[addr]; ok {
		log.Debug("get producer ", addr)
		return pro, nil
	}
	return nil, fmt.Errorf("no nsqd server avaiable")
}

func (qm *NSQManager) Enqueue(name string, evt interface{}) {
	log.Info("nsq publish ", name)
	p, err := qm.GetOneProducer()
	if err == nil {
		evtMsg, err := json.Marshal(evt)
		if err != nil {
			log.Error("json marshal: ", err.Error())
			return
		}
		err = p.Publish(name, evtMsg)
		if err != nil {
			log.Error("nsq publish: ", err.Error())
			return
		}
	} else {
		log.Error("nsq enqueue error: ", err.Error())
	}
}

func (qm *NSQManager) NewNSQConsumer(topic, channel string, concurrency int) (*nsq.Consumer, error) {
	log.Infof("new consumer %s/%s", topic, channel)
	c, err := nsq.NewConsumer(topic, channel, qm.config)
	if err != nil {
		log.Error("nsq new comsumer: ", err.Error())
		return c, err
	}
	err = c.ConnectToNSQLookupds(qm.lookupdAddrs)
	if err != nil {
		log.Error("nsq connect to nsq lookupds: ", err.Error())
		return c, err
	}
	return c, nil
}

func (qm *NSQManager) PauseTopic(topicname string) error {
	qs := fmt.Sprintf("topic=%s", topicname)
	return qm.actionHelper(topicname, "topic", "pause", qs)
}

func (qm *NSQManager) UnPauseTopic(topicname string) error {
	qs := fmt.Sprintf("topic=%s", topicname)
	return qm.actionHelper(topicname, "topic", "unpause", qs)
}

func (qm *NSQManager) actionHelper(topicname, url, action, qs string) error {
	pros, err := qm.GetProducer(topicname)
	if err != nil {
		return err
	}
	for _, pro := range pros {
		endpoint := fmt.Sprintf("http://%s:%d/%s/%s?%s", pro.BroadcastAddress, pro.HTTPPort, url, action, qs)
		resp, err := http.PostForm(endpoint, nil)
		if err != nil {
			log.Error("http post form error: ", err.Error())
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Error("pause topic error: ", err.Error())
			continue
		}
		if resp.StatusCode != 200 {
			log.Error("pause topic error: %q", body)
			continue
		}
	}
	return nil
}
