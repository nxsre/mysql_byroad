package main

import (
	"log"

	"encoding/json"
	"io/ioutil"

	"github.com/Shopify/sarama"
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

func (this *Entity) Encode() ([]byte, error) {
	return json.Marshal(this)
}
func (this *Entity) Length() int {
	data, _ := this.Encode()
	return len(data)
}

func main() {
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	data, err := ioutil.ReadFile("data.json")
	if err != nil {
		log.Panic(err)
	}
	entity := Entity{}
	err = json.Unmarshal(data, &entity)
	if err != nil {
		log.Panic(err)
	}
	msg := &sarama.ProducerMessage{Topic: entity.Database + "___" + entity.Table, Value: &entity}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}
}
