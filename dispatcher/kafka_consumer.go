package main

import (
	"encoding/json"
	"fmt"
	"mysql_byroad/model"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kafka/consumergroup"
)

type Entity struct {
	Database      string  `json:"database"`
	Table         string  `json:"table"`
	BeforeColumns Columns `json:"beforeColumns"`
	AfterColumns  Columns `json:"afterColumns"`
	EventType     string  `json:"eventType"`
	ExecuteTime   int64   `json:"executeTime"`
}

type Columns []*Column

type Column struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	SqlType int    `json:"sqlType"`
	IsKey   bool   `json:"isKey"`
	IsNull  bool   `json:"isNull"`
	Updated bool   `json:"updated"`
}

func (columns Columns) String() string {
	var ret string
	for _, column := range columns {
		ret += fmt.Sprintf("%+v ", column)
	}

	return ret
}

func (entity *Entity) String() string {
	ret := fmt.Sprintf("%s.%s:%s[%+s->%+s]", entity.Database, entity.Table, entity.EventType, entity.BeforeColumns.String(), entity.AfterColumns.String())
	return ret
}

func (entity *Entity) Encode() ([]byte, error) {
	return json.Marshal(entity)
}

func (entity *Entity) Length() int {
	data, err := json.Marshal(entity)
	if err != nil {
		return 0
	}
	return len(data)
}

type KafkaHandler interface {
	HandleKafkaEvent(entity *Entity)
}

type KafkaConsumer struct {
	Topics   []string
	GroupID  string
	consumer *consumergroup.ConsumerGroup
	handlers []KafkaHandler
}

/*
新建kafka consumer，使用consumer group的方式订阅topic
*/
func NewKafkaConsumer(topics []string, groupid string, kafkaconfig KafkaConfig) (*KafkaConsumer, error) {
	kconsumer := KafkaConsumer{
		Topics:   topics,
		GroupID:  groupid,
		handlers: make([]KafkaHandler, 0, 1),
	}
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = kafkaconfig.OffsetProcessingTimeout.Duration
	config.Offsets.ResetOffsets = kafkaconfig.OffsetResetOffsets
	consumer, err := consumergroup.JoinConsumerGroup(groupid, topics, kafkaconfig.ZkAddrs, config)
	log.Debugf("new kafka consumers for %s, %+v", groupid, topics)
	if err != nil {
		return nil, err
	}
	kconsumer.consumer = consumer
	return &kconsumer, nil
}

func (kconsumer *KafkaConsumer) HandleMessage() {
	go func() {
		for msg := range kconsumer.consumer.Messages() {
			log.Debugf("receive consumer message")
			entity := Entity{}
			err := json.Unmarshal(msg.Value, &entity)
			if err != nil {
				log.Errorf("kafka consumer unmarshal error: %s", err.Error())
			}
			for _, handler := range kconsumer.handlers {
				handler.HandleKafkaEvent(&entity)
			}
			err = kconsumer.consumer.CommitUpto(msg)
			if err != nil {
				log.Errorf("kafka commitupto error: %s", err.Error())
			}
		}
	}()
}

func (kconsumer *KafkaConsumer) AddHandler(handler KafkaHandler) {
	kconsumer.handlers = append(kconsumer.handlers, handler)
}

func (kconsumer *KafkaConsumer) Close() error {
	return kconsumer.consumer.Close()
}

type KafkaConsumerManager struct {
	consumers map[string]*KafkaConsumer
	sync.RWMutex
	config   KafkaConfig
	handlers []KafkaHandler
}

func NewKafkaConsumerManager(config KafkaConfig) *KafkaConsumerManager {
	manager := KafkaConsumerManager{
		consumers: make(map[string]*KafkaConsumer),
		config:    config,
	}
	return &manager
}

func (kcm *KafkaConsumerManager) Add(kc *KafkaConsumer) {
	kcm.Lock()
	kcm.consumers[kc.GroupID] = kc
	kcm.Unlock()
}

func (kcm *KafkaConsumerManager) Delete(kc *KafkaConsumer) {
	kcm.Lock()
	delete(kcm.consumers, kc.GroupID)
	kcm.Unlock()
}

func (kcm *KafkaConsumerManager) Get(groupid string) *KafkaConsumer {
	kcm.RLock()
	defer kcm.RUnlock()
	return kcm.consumers[groupid]
}

func (kcm *KafkaConsumerManager) Iter() <-chan *KafkaConsumer {
	ch := make(chan *KafkaConsumer)
	go func() {
		kcm.RLock()
		for _, kc := range kcm.consumers {
			ch <- kc
		}
		kcm.RUnlock()
		close(ch)
	}()
	return ch
}

func (kcm *KafkaConsumerManager) IterBuffered() <-chan *KafkaConsumer {
	ch := make(chan *KafkaConsumer, kcm.Len())
	go func() {
		kcm.RLock()
		for _, kc := range kcm.consumers {
			ch <- kc
		}
		kcm.RUnlock()
		close(ch)
	}()
	return ch
}

func (kcm *KafkaConsumerManager) Len() int {
	kcm.RLock()
	length := len(kcm.consumers)
	kcm.RUnlock()
	return length
}

func (kcm *KafkaConsumerManager) GroupExists(groupid string) bool {
	kcm.RLock()
	_, ok := kcm.consumers[groupid]
	kcm.RUnlock()
	return ok
}

/*
根据任务的数据库-表信息，新建kafka consumer
*/
func (kcm *KafkaConsumerManager) InitConsumers(tasks []*model.Task) {
	wg := sync.WaitGroup{}
	for _, task := range tasks {
		if task.Stat == model.TASK_STATE_START {
			wg.Add(1)
			go func(t *model.Task) {
				for _, handler := range kcm.handlers {
					kcm.traverseTask(t, handler)
				}
				wg.Done()
			}(task)
		}
	}
	wg.Wait()
}

/*
添加handler
*/
func (kcm *KafkaConsumerManager) AddHandler(handler KafkaHandler) {
	kcm.handlers = append(kcm.handlers, handler)
}

/*
停止所有的kafka consumer
*/
func (kcm *KafkaConsumerManager) StopConsumers() {
	wg := sync.WaitGroup{}
	for consumer := range kcm.Iter() {
		wg.Add(1)
		go func(c *KafkaConsumer) {
			log.Debugf("close consumer %s", c.GroupID)
			err := c.Close()
			if err != nil {
				log.Errorf("kafka consumer close error: %s", err.Error())
			}
			wg.Done()
		}(consumer)
	}
	wg.Wait()
}

/*
遍历任务订阅的所有字段信息，为没有订阅kafka相应topic的字段添加consumer
*/
func (kcm *KafkaConsumerManager) AddTask(task *model.Task) {
	for _, handler := range kcm.handlers {
		kcm.traverseTask(task, handler)
	}
}

/*
根据任务订阅的字段信息，更新consumer
*/
func (kcm *KafkaConsumerManager) UpdateTask(task *model.Task) {
	groupid := GenGroupID(task)
	consumer := kcm.Get(groupid)
	if consumer != nil {
		consumer.consumer.Close()
		kcm.Delete(consumer)
	}
	for _, handler := range kcm.handlers {
		kcm.traverseTask(task, handler)
	}
}

func (kcm *KafkaConsumerManager) StartTask(task *model.Task) {
	kcm.AddTask(task)
}

// 停止订阅，从consumerManager中删除相应的consumer
func (kcm *KafkaConsumerManager) StopTask(task *model.Task) {
	groupid := GenGroupID(task)
	consumer := kcm.Get(groupid)
	consumer.Close()
	kcm.Delete(consumer)
}

func (kcm *KafkaConsumerManager) DeleteTask(task *model.Task) {
	kcm.StopTask(task)
}

// 遍历任务的字段信息，增加相应的订阅
func (kcm *KafkaConsumerManager) traverseTask(task *model.Task, handler KafkaHandler) {
	topics := kcm.getTopics(task)
	if len(topics) == 0 {
		log.Errorf("no matched kafka topic found for %s!", task.Name)
		return
	}
	groupid := GenGroupID(task)
	if !kcm.GroupExists(groupid) {
		consumer, err := NewKafkaConsumer(topics, groupid, kcm.config)
		if err != nil {
			log.Errorf("new kafka consumer error: %s", err.Error())
			return
		}
		consumer.AddHandler(handler)
		kcm.Add(consumer)
	}
}

type empty struct{}

// 得到任务订阅的字段信息对应的所有的topic
func (kcm *KafkaConsumerManager) getTopics(task *model.Task) []string {
	topics := make([]string, 0, 10)
	set := make(map[string]empty)
	allTopics, err := kcm.getAllTopics()
	if err != nil {
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
func (kcm *KafkaConsumerManager) getAllTopics() ([]string, error) {
	conn, _, err := zk.Connect(kcm.config.ZkAddrs, time.Second)
	if err != nil {
		return nil, err
	}
	children, _, err := conn.Children("/brokers/topics")
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
