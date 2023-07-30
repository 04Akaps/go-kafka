package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
)

type Kafka struct {
	producer    *kafka.Producer
	consumerMap *kafka.Consumer
}

func NewKafkaProducer(producerConf *kafka.ConfigMap) (*Kafka, error) {
	var kafkaResult Kafka
	var err error

	if kafkaResult.producer, err = kafka.NewProducer(producerConf); err != nil {
		return nil, err
	} else {
		return &kafkaResult, nil
	}
}

func (k *Kafka) AddNewConsumer(topic string, conf *kafka.ConfigMap) error {
	client, err := kafka.NewConsumer(conf)
	if err != nil {
		return err
	}

	k.consumerMap = client

	err = k.consumerMap.Subscribe(topic, rebalanceCallback)

	if err != nil {
		return err
	}

	return nil
}

func (k *Kafka) ConsumeEvent(topic string) error {
	for {

		ev := k.consumerMap.Poll(100)

		switch event := ev.(type) {
		case *kafka.Message:
			var consumeValue TestStruct

			err := json.Unmarshal(event.Value, &consumeValue)
			if err != nil {
				msg := fmt.Sprint("UnMarshal Err : ", err)
				panic(msg)
			}

			fmt.Println(consumeValue.Age)
			fmt.Println(consumeValue.Name)

			// fmt.Printf("New Event From Kafka Queue : %s , partition : %s  offset : %s\n", string(event.Value), event.TopicPartition, event.TopicPartition.Offset)
			// fmt.Println("")
			// fmt.Printf("Partition Meta : %d", event.TopicPartition.Partition)
			// fmt.Println("")
			// _, err = k.consumerMap.CommitMessage(event)
			// if err != nil {
			// 	fmt.Printf("Error committing offset: %s\n", err)
			// }

			// k.consumerMap.StoreMessage(event)

			// CommitMessage -> Offset을 저장한다.
			// 그러기 떄문에 특정 로직을 수행하고, 해당 로직이 정상적으로 완료가 되었을 떄만 실행이 되어야 한다.

			// StoreMessage -> Offset을 저장하지 않는다.
			// 보통 로직을 실행하다가 에러가 발생 했을 경우, 해당 함수를 실행하고,
			// 따로 에러를 처리한다.

		case *kafka.Error:
			fmt.Printf("%s\n", event.Error())
		}
	}
}

func (k *Kafka) ConsumeAvroEvent() {
	for {

		ev := k.consumerMap.Poll(100)

		switch event := ev.(type) {
		case *kafka.Message:

			ocfReader, err := goavro.NewOCFReader(bytes.NewReader(event.Value))
			if err != nil {
				fmt.Println(err)
			}

			for ocfReader.Scan() {
				record, err := ocfReader.Read()
				if err != nil {
					fmt.Println(err)
				}

				dataMap, ok := record.(map[string]interface{})
				if !ok {
					// 형변환 실패
					fmt.Println("Failed to convert to map[string]interface{}")
					continue
				}

				fmt.Println("FieldOne : ", dataMap["field1"].(string))
				fmt.Println("FieldTwo : ", dataMap["field2"].(int32))
				fmt.Println("")
			}

		case *kafka.Error:
			fmt.Printf("%s\n", event.Error())
		}
	}
}

func (k *Kafka) SendEvent(topic *string, ch chan kafka.Event, value []byte) error {
	if err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
		Value:          value,
	},
		ch,
	); err != nil {
		return err
	} else {
		// TODO 테스트 용
		// k.ProducerLen()

		chResult := <-ch
		fmt.Println(chResult)

		return nil
	}
}

func (k *Kafka) SendEventWithKey(topic *string, ch chan kafka.Event, value []byte) error {
	if err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
		Value:          value,
		Key:            []byte("My_Dummy_Key"),
	},
		ch,
	); err != nil {
		return err
	} else {
		// TODO 테스트 용
		// k.ProducerLen()

		chResult := <-ch
		fmt.Println(chResult)

		return nil
	}
}

func (k *Kafka) ProducerLen() {
	// 큐에 대기중인 메시지의 갯수
	// SendEvent에서  <- ch 가 실행이 되면, 전송이 완료가 되는 것으로,
	// 큐에서 제거가 된다.
	fmt.Println(k.producer.Len())
}

func (k *Kafka) MetaData() {
	// 프로듀서의 메타데이터를 가져오기 위한 함수
	// GetMetadata라는 함수의 첫번쨰 파라메터로 topic을 주면,
	// 해당 토픽에 대한 값만 나온다.
	metaData, err := k.producer.GetMetadata(nil, true, 5000)
	if err != nil {
		panic(err)
	}

	for _, topic := range metaData.Topics {
		fmt.Printf("- Topic: %s, Partitions: %d\n", *&topic.Topic, len(topic.Partitions))
	}

	for _, broker := range metaData.Brokers {
		fmt.Printf("- Broker ID: %d, Address: %s\n, Port: %d\n", broker.ID, broker.Host, broker.Port)
	}
}

func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		fmt.Printf("%% %s rebalance: %d new partition(s) assigned: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

		err := c.Assign(ev.Partitions)
		// IncrementalAssign -> 새롭게 추가되는 파티션만 할당
		// 현재 발생한 파티션은 할당 x
		// -> 보통 수동으로 파티션을 관리할 떄 사용이 된다고 한다.
		// Assign -> 현재 추가되는 파티션 부터 할당
		if err != nil {
			return err
		}

	case kafka.RevokedPartitions:
		fmt.Printf("%% %s rebalance: %d partition(s) revoked: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

		if c.AssignmentLost() {
			fmt.Fprintln(os.Stderr, "Assignment lost involuntarily, commit may fail")
		}

		commitedOffsets, err := c.Commit()

		if err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
			fmt.Fprintf(os.Stderr, "Failed to commit offsets: %s\n", err)
			return err
		}
		fmt.Printf("%% Commited offsets to Kafka: %v\n", commitedOffsets)

	default:
		fmt.Fprintf(os.Stderr, "Unxpected event type: %v\n", event)
	}

	return nil
}

// if e.Code() == kafka.ErrAllBrokersDown {
// 	run = false
// }

// if err.(kafka.Error).Code() == kafka.ErrQueueFull {
// 	// Producer queue is full, wait 1s for messages
// 	// to be delivered then try again.
// 	time.Sleep(time.Second)
// 	continue
// }
