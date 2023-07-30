package main

import (
	"bytes"
	"fmt"
	"time"

	kc "go-kafka/kafka"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
)

func main() {
	var ocfFileContents bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &ocfFileContents,
		Schema: kc.AvroSchema,
	})

	if err != nil {
		fmt.Println("NewOCFWriter ")
		panic(err)
	}

	err = writer.Append([]map[string]interface{}{
		{
			"field1": "First Name",
			"field2": 123,
		},
		{
			"field1": "Second Name",
			"field2": 4565,
		},
	})

	if err != nil {
		fmt.Println("여기 들어옴")
		fmt.Println(err)
	}

	producerConf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091",
		"client.id":         "producer-name-first",
		"acks":              "all",
	}

	consumerConf := &kafka.ConfigMap{
		"bootstrap.servers":             "localhost:9091",
		"group.id":                      "consumer_group_3",
		"auto.offset.reset":             "latest",
		"partition.assignment.strategy": "roundrobin",
	}

	topic := "Avro_Topic"

	if client, err := kc.NewKafkaProducer(producerConf); err != nil {
		fmt.Println("Producer", err)
		return
	} else if err = client.AddNewConsumer(topic, consumerConf); err != nil {
		fmt.Println("Consumer", err)
		return
	} else {
		// client.MetaData()

		go client.ConsumeAvroEvent()

		for {

			ch := make(chan kafka.Event)

			if err != nil {
				fmt.Println("Failed to serialize data to JSON:", err)
				return
			}

			// value := []byte("test")

			if err = client.SendEvent(&topic, ch, ocfFileContents.Bytes()); err != nil {
				fmt.Println("Send Err", err)
				panic(err)
			}

			time.Sleep(3 * time.Second)

		}
	}
}
