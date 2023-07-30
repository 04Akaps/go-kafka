package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"time"

	kc "go-kafka/kafka"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type CustomPartitioner struct{}

func (p *CustomPartitioner) Partition(msg *kafka.Message, numPartitions int32) int32 {
	// Check if the message has a key.
	if msg.Key == nil {
		return -1 // Return -1 for messages without a key.
	}

	// Compute the hash value from the key bytes.
	hash := hashBytes(msg.Key)

	// Calculate the partition based on the hash value.
	partition := int32(hash) % numPartitions

	return partition
}

func hashBytes(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func main() {
	producerConf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091",
		"client.id":         "producer-name-first",
		"acks":              "all",
		"partitioner":       &CustomPartitioner{},
		// "delivery.timeout.ms": 1000,
		// "request.timeout.ms":  300000,
	}

	consumerConf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091",
		"group.id":          "consumer_group_3",
		"auto.offset.reset": "latest",
		// "go.application.rebalance.enable": true,
		// "enable.auto.commit":              false,
		"partition.assignment.strategy": "roundrobin",
	}
	// go.application.rebalance.enable 설정을 추가하면
	// 새롭게 추가되는 consumer에 대해서, 파티션을 리밸런싱 한다.

	// auto.commit은 기본적으로 false로 두는 것이 좋다.
	// 재시도 로직을 구현 할 수 있기 떄문에

	// latest
	// earliest

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
