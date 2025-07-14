package kafka_whatsapp

import (
	"encoding/json"
	"kafka-whatsapp/topics"
	"time"

	"github.com/dev-star-company/custom-validate/validate"
	"github.com/segmentio/kafka-go"
)

type Status struct {
	Status string
	Client string
	At     time.Time
}

func NewStatus(status, client string, at time.Time) (*Status, error) {
	fields := map[string]string{
		"Status": "required",
		"Client": "required",
		"At":     "required",
	}

	msg := &Status{
		Status: status,
		Client: client,
		At:     at,
	}

	if err := validate.Validate(fields, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func (p *kafka_whatsapper) PublishToStatus(message Message[Status]) error {
	conn, err := p.Connect(topics.WHATSAPP_STATUS)
	if err != nil {
		return err
	}

	msgBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: msgBytes},
	)
	if err != nil {
		return err
	}

	if err := conn.Close(); err != nil {
		return err
	}
	return nil
}
