package kafka_whatsapp

import (
	"context"
	"fmt"
	"sync"

	"github.com/dev-star-company/kafka-whatsapp/topics"

	"github.com/segmentio/kafka-go"
)

type Message[T WhatsappMsg | Status] struct {
	Payload   T      `json:"object"`    // any of the SyncSomethingStruct types
	Publisher string `json:"publisher"` // the name of the publisher
}

type kafka_whatsapper struct {
	kafka_whatsapps sync.Map
	consumerGroupID string
	brokerUrl       string
}

type SubResponse[T WhatsappMsg | Status] struct {
	Message  Message[T]                                             `json:"message"`   // The message received from the topic
	CommitFn func(ctx context.Context, msgs ...kafka.Message) error `json:"commit_fn"` // The commit function to call after processing the message
}

// ConsumerGroupId is the ID of the consumer group that will be used for subscribing to topics.
// It is used to ensure that multiple consumers can read from the same topic without duplicating messages.
// It is important to set this ID when creating a new kafka_whatsapper instance.
// Should be set to a unique value for each consumer group.
func New(brokerUrl string, consumerGroupID string) *kafka_whatsapper {
	return &kafka_whatsapper{
		kafka_whatsapps: sync.Map{},
		consumerGroupID: consumerGroupID,
		brokerUrl:       brokerUrl,
	}
}

// Use topics from topics package
func (c *kafka_whatsapper) ConnectToTopic(topic topics.Topic) (*kafka.Conn, error) {
	if conn, ok := c.kafka_whatsapps.Load(topic); ok {
		return conn.(*kafka.Conn), nil
	}

	conn, err := c.Connect(topic)
	if err != nil {
		return nil, err
	}

	c.kafka_whatsapps.Store(topic, conn)
	return conn, nil
}

func (c *kafka_whatsapper) Connect(topic topics.Topic) (*kafka.Conn, error) {
	fmt.Println(c.brokerUrl, string(topic))
	conn, err := kafka.DialLeader(context.Background(), "tcp", c.brokerUrl, string(topic), 0)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
