package main

import (
	"encoding/json"
	"fmt"
	"time"

	kc "go-kafka/kafka"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producerConf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091",
		"client.id":         "producer-name-first",
		"acks":              "all",
		// "delivery.timeout.ms": 1000,
		// "request.timeout.ms":  300000,
	}

	consumerConf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091",
		"group.id":          "consumer_group_3",
		"auto.offset.reset": "latest",
		// "go.application.rebalance.enable": true,
		"enable.auto.commit": false,
		// "partition.assignment.strategy": "roundrobin",
	}

	topic := "Test_Topic"

	if client, err := kc.NewKafkaProducer(producerConf); err != nil {
		fmt.Println("Producer", err)
		return
	} else if err = client.AddNewConsumer(topic, consumerConf); err != nil {
		fmt.Println("Consumer", err)
		return
	} else {
		// client.MetaData()

		go client.ConsumeEvent(topic)

		for {

			ch := make(chan kafka.Event)

			value, err := json.Marshal(
				kc.TestStruct{
					Name: "testName",
					Age:  333,
				},
			)

			if err != nil {
				fmt.Println("Failed to serialize data to JSON:", err)
				return
			}

			// value := []byte("test")

			if err = client.SendEvent(&topic, ch, value); err != nil {
				fmt.Println("Send Err", err)
				panic(err)
			}

			time.Sleep(3 * time.Second)

		}
	}
}
