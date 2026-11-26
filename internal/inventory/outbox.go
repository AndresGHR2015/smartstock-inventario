package inventory

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Publisher interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

// PublishOne leaves the event pending on failure. A crash after Kafka accepts
// the message but before commit can cause redelivery with the same event ID.
func PublishOne(ctx context.Context, db *pgxpool.Pool, publisher Publisher) (bool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id, sku string
	var payload []byte
	err = tx.QueryRow(ctx, "SELECT event_id, sku, payload FROM outbox WHERE published_at IS NULL ORDER BY event_id FOR UPDATE SKIP LOCKED LIMIT 1").
		Scan(&id, &sku, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err = publisher.WriteMessages(ctx, kafka.Message{Key: []byte(sku), Value: payload}); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, "UPDATE outbox SET published_at=now() WHERE event_id=$1", id); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
