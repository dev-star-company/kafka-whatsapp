package kafka_whatsapp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dev-star-company/kafka-whatsapp/topics"

	"github.com/segmentio/kafka-go"
)

func (p *KafkaWhatsapper) SubToStatus(ctx context.Context) (<-chan SubResponse[Status], error) {
	ch := make(chan SubResponse[Status])
	conn, err := p.ConnectToTopic(topics.WHATSAPP_STATUS)
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
		GroupID:  p.consumerGroupID,
		Topic:    string(topics.WHATSAPP_STATUS),
		MaxBytes: 10 * 1024 * 1024, // 10 MB
	})

	go func() {
		defer close(ch)
		defer r.Close()
		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				return // Exit on context cancellation or reader close
			}
			var status Message[Status]
			if err := json.Unmarshal(msg.Value, &status); err != nil {
				continue // skip invalid messages
			}
			if status.Publisher == p.consumerGroupID {
				continue // skip messages from the same publisher
			}
			select {
			case ch <- SubResponse[Status]{Message: status, CommitFn: func() error { return r.CommitMessages(ctx, msg) }}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}
