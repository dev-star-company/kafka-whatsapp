package kafka_whatsapp

import (
	"encoding/json"
	"time"

	"github.com/dev-star-company/kafka-whatsapp/topics"

	"github.com/dev-star-company/custom-validate/validate"
	"github.com/segmentio/kafka-go"
)

type WhatsappMsg struct {
	Uuid        string
	From        string
	Client      string
	Recipient   string
	MessageType string
	Message     string
	IsFromMe    bool
	IsGroup     bool
	ReceivedAt  time.Time
}

func NewWhatsappMsg(uuid, from, client, recipient, messageType, message string, isFromMe, isGroup bool, receivedAt time.Time) (*WhatsappMsg, error) {
	fields := map[string]string{
		"Uuid":        "required,uuid",
		"From":        "required",
		"Client":      "required",
		"Recipient":   "required",
		"MessageType": "required",
		"Message":     "Required",
		"IsFromMe":    "required,boolean",
		"IsGroup":     "required,boolean",
		"ReceivedAt":  "required",
	}

	msg := &WhatsappMsg{
		Uuid:        uuid,
		From:        from,
		Client:      client,
		Recipient:   recipient,
		MessageType: messageType,
		Message:     message,
		IsFromMe:    isFromMe,
		IsGroup:     isGroup,
		ReceivedAt:  receivedAt,
	}

	if err := validate.Validate(fields, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func (p *kafka_whatsapper) PublishToWhatsappMsg(message Message[WhatsappMsg]) error {
	conn, err := p.Connect(topics.WHATSAPP_MESSAGES)
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
