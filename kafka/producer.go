package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
)

var ProducerConfig = sarama.NewConfig()
var BrokerList []string
var ClientId string

func ProduceMessage(msg *sarama.ProducerMessage, count int, topic string) {
	ProducerConfig.Version = sarama.V0_11_0_2
	ProducerConfig.Producer.RequiredAcks = sarama.WaitForAll
	ProducerConfig.Producer.Retry.Max = 5
	ProducerConfig.Producer.MaxMessageBytes = 304857600
	ProducerConfig.Producer.Return.Successes = true
	ProducerConfig.ClientID = ClientId

	producer, err := sarama.NewSyncProducer(BrokerList, ProducerConfig)
	if err != nil {
		// Should not reach here
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			// Should not reach here
			panic(err)
		}
	}()

	if count > 1 {
		for i := 0; i < count; i++ {
			sendMessage(msg, producer, topic)
		}
	} else {
		sendMessage(msg, producer, topic)
	}
}

func sendMessage(msg *sarama.ProducerMessage, producer sarama.SyncProducer, topic string) {
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
}
