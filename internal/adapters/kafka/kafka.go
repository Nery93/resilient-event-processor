package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
	"resilient-event-processor/internal/domain"
)

type Producer struct {
	writer *kafka.Writer
}



func (p *Producer) Publish(ctx context.Context, event domain.EventRequest) error {
	msg := kafka.Message{
		Key:   []byte(event.EventID),
		Value: []byte(event.Payload),
		Headers: []kafka.Header{
			{
				Key:   "Idempotency-Key",
				Value: []byte(event.EventID),
			},
		},
	}
	return p.writer.WriteMessages(ctx, msg)
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topic,
		},
	}
}
