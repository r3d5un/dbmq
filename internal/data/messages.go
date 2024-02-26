package data

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Message struct {
	ID        *int64     `json:"id,omitempty"`
	Data      *Data      `json:"data,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
}

type Data struct {
	MessageBody string `json:"message"`
}

type MessageModel struct {
	Pool *pgxpool.Pool
}

func (m MessageModel) Insert(tx pgx.Tx, data Data) (*Message, error) {
	slog.Error("not implemented")
	return nil, nil
}

func (m MessageModel) GetNext(tx pgx.Tx) (*Message, error) {
	stmt := `SELECT id, data, created_at
        FROM messages
        ORDER BY created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1;`

	var msg Message
	var rawData []byte

	err := tx.QueryRow(context.Background(), stmt).Scan(
		&msg.ID,
		&rawData,
		&msg.CreatedAt,
	)
	if err != nil {
		switch err {
		case pgx.ErrNoRows:
			slog.Info("no new messages", "error", err)
			return nil, nil
		default:
			slog.Error("unable to execute statement", "error", err)
			return nil, err
		}
	}

	err = json.Unmarshal(rawData, &msg.Data)
	if err != nil {
		slog.Error("unable to unmarshal data", "error", err)
		return nil, err
	}

	return &msg, nil
}

func (m MessageModel) Delete(tx *pgx.Tx, id int64) error {
	slog.Error("not implemented")
	return nil
}
