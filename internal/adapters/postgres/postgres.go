package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db *pgxpool.Pool
}

// EventProcessedQuery is the SQL query to mark an event as processed in the database.
const (
	EventProcessedQuery = `
		INSERT INTO processed_events (event_id)
		VALUES ($1)
	`
)

// HandleEventProcessed marks an event as processed in the database.
func (s *Store) HandleEventProcessed(eventID string) error {

	_, err := s.db.Exec(context.Background(), EventProcessedQuery, eventID)
	if err != nil {
		return err
	}
	return nil
}

// NewStore creates a new instance of the Store with the given database connection.
func NewStore(db *pgxpool.Pool) *Store {
	return &Store{
		db: db,
	}
}
