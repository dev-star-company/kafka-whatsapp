package kafka_whatsapp

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-whatsapp/topics"
	"time"

	"github.com/segmentio/kafka-go"
)

func (c *kafka_whatsapper) SubscribeToWhatsappMsg(ctx context.Context) (<-chan SubResponse[WhatsappMsg], error) {
	ch := make(chan SubResponse[WhatsappMsg])
	conn, err := c.ConnectToTopic(topics.WHATSAPP_MESSAGES)
	if err != nil {
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	ctrl, err := conn.Controller()
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s:%d", ctrl.Host, ctrl.Port)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{url},
		GroupID:  c.consumerGroupID,
		Topic:    string(topics.WHATSAPP_MESSAGES),
		MaxBytes: 10 * 1024 * 1024, // 10 MB
	})

	go func() {
		defer close(ch)
		defer r.Close()
		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				// Exit on context cancellation or reader close
				return
			}
			var user Message[WhatsappMsg]
			if err := json.Unmarshal(msg.Value, &user); err != nil {
				continue // skip invalid messages
			}
			if user.Publisher == c.consumerGroupID {
				continue // skip messages from the same publisher
			}
			select {
			case ch <- SubResponse[WhatsappMsg]{Message: user, CommitFn: func() error { return r.CommitMessages(ctx, msg) }}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}
