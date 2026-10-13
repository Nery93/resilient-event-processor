package ports

import (
	"context"

	"resilient-event-processor/internal/domain"
)

type EventPublisher interface {
	Publish(ctx context.Context, event domain.Event) error
}

type EventProcessed interface {
	HandleEventProcessed(ctx context.Context, event domain.Event) (bool, error)
}