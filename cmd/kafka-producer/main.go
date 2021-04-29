package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"strconv"
	"time"
)

const (
	broker = "localhost:9092"
	topic = "messages"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
					} else {
						fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := topic
	i := 1
	for {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key: 			[]byte(strconv.Itoa(i)),
			Value:          []byte("Message " + strconv.Itoa(i)),
		}, nil)
		i++

		time.Sleep(time.Second)
	}
}
