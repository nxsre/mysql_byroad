package main

import (
	"encoding/json"
	"fmt"
	"mysql_byroad/model"
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
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
	Value   string `json:"string"`
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
	Database string
	Table    string
	consumer *consumergroup.ConsumerGroup
	handlers []KafkaHandler
}

/*
新建kafka consumer，使用consumer group的方式订阅topic
*/
func NewKafkaConsumer(database, table string, zookeeper []string) (*KafkaConsumer, error) {
	kconsumer := KafkaConsumer{
		Database: database,
		Table:    table,
		handlers: make([]KafkaHandler, 0, 1),
	}
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	topic := kconsumer.GetTopic()
	consumer, err := consumergroup.JoinConsumerGroup(topic, []string{topic}, zookeeper, config)
	log.Debugf("new kafka consumer topic: %s", topic)
	if err != nil {
		return nil, err
	}
	kconsumer.consumer = consumer
	return &kconsumer, nil
}

func (kconsumer *KafkaConsumer) GetTopic() string {
	return GenTopicName(kconsumer.Database, kconsumer.Table)
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
	zkaddrs []string
}

func NewKafkaConsumerManager(zkaddrs []string) *KafkaConsumerManager {
	manager := KafkaConsumerManager{
		consumers: make(map[string]*KafkaConsumer),
		zkaddrs:   zkaddrs,
	}
	return &manager
}

func (kcm *KafkaConsumerManager) Add(kc *KafkaConsumer) {
	kcm.Lock()
	kcm.consumers[kc.GetTopic()] = kc
	kcm.Unlock()
}

func (kcm *KafkaConsumerManager) Delete(kc *KafkaConsumer) {
	kcm.Lock()
	delete(kcm.consumers, kc.GetTopic())
	kcm.Unlock()
}

func (kcm *KafkaConsumerManager) Iter() <-chan *KafkaConsumer {
	ch := make(chan *KafkaConsumer)
	go func() {
		kcm.RLock()
		for _, kc := range kcm.consumers {
			ch <- kc
		}
		kcm.Unlock()
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
		kcm.Unlock()
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

func (kcm *KafkaConsumerManager) TopicExists(topic string) bool {
	kcm.RLock()
	_, ok := kcm.consumers[topic]
	kcm.RUnlock()
	return ok
}

/*
根据任务的数据库-表信息，新建kafka consumer
*/
func (kcm *KafkaConsumerManager) InitConsumers(tasks []*model.Task) {
	for _, task := range tasks {
		for _, field := range task.Fields {
			topic := GenTopicName(field.Schema, field.Table)
			if !kcm.TopicExists(topic) {
				consumer, err := NewKafkaConsumer(field.Schema, field.Table, kcm.zkaddrs)
				if err != nil {
					log.Errorf("new kafka consumer error: %s", err.Error())
					continue
				}
				kcm.Add(consumer)
			}
		}
	}
}

/*
添加handler并且开始从kafka获取消息进行处理
*/
func (kcm *KafkaConsumerManager) AddHandler(handler KafkaHandler) {
	for consumer := range kcm.Iter() {
		consumer.AddHandler(handler)
		consumer.HandleMessage()
	}
}

/*
停止所有的kafka consumer
*/
func (kcm *KafkaConsumerManager) StopConsumers() {
	for consumer := range kcm.Iter() {
		err := consumer.Close()
		if err != nil {
			log.Errorf("kafka consumer close error: %s", err.Error())
			continue
		}
	}
}
