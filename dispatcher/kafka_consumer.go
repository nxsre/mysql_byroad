package main

import (
	"encoding/json"
	"fmt"

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

type KafkaConsumer struct {
	Database string
	Table    string
	consumer *consumergroup.ConsumerGroup
}

func NewKafkaConsumer(database, table string, zookeeper []string) (*KafkaConsumer, error) {
	kconsumer := KafkaConsumer{
		Database: database,
		Table:    table,
	}
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	topic := kconsumer.GetTopic()
	consumer, err := consumergroup.JoinConsumerGroup(topic, []string{topic}, zookeeper, config)
	if err != nil {
		return nil, err
	}
	kconsumer.consumer = consumer
	return &kconsumer, nil
}

func (kconsumer *KafkaConsumer) GetTopic() string {
	return kconsumer.Database + "___" + kconsumer.Table
}

func (kconsumer *KafkaConsumer) HandleMessage() {
	for msg := range kconsumer.consumer.Messages() {
		entity := Entity{}
		err := json.Unmarshal(msg.Value, &entity)
		if err != nil {
			log.Panic(err)
		}

		log.Println(entity.String())
	}
}
