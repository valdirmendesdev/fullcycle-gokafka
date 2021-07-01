package main

import(
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	producer := NewKafkaProducer()
	Publish("mensagem", "teste", producer, nil)
	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka_kafka_1:9092",
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Fatal(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte) error {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:          []byte(msg),
		Key:            key,
	}
	err := producer.Produce(message, nil)
	if err != nil {
		return err
	}
	return nil
}