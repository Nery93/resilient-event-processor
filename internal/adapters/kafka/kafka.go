package kafka

import (
	"encoding/json"
	"context"

	"github.com/segmentio/kafka-go"
	"resilient-event-processor/internal/domain"
)

type Producer struct {
	writer *kafka.Writer
}



func (p *Producer) Publish(ctx context.Context, event domain.Event) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.EventID),
		Value: value,
		Time: event.Timestamp,
		Headers: []kafka.Header{
			{Key: "Idempotency-Key", Value: []byte(event.EventID)},
		},
	})
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topic,
		},
	}
}
