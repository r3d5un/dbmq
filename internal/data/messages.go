package data

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

func (m MessageModel) NewMessage(tx pgx.Tx, data Data) (*Message, error) {
	stmt := `INSERT INTO messages (data)
        VALUES ($1)
        RETURNING id, data, created_at;`

	dataJSON, err := json.Marshal(data)
	if err != nil {
		slog.Error("unable to marshal data", "error", err)
		return nil, err
	}

	var msg Message
	var rawData []byte

	err = tx.QueryRow(context.Background(), stmt, dataJSON).Scan(
		&msg.ID,
		&rawData,
		&msg.CreatedAt,
	)
	if err != nil {
		slog.Error("unable to execute statement", "error", err)
		return nil, err
	}

	err = json.Unmarshal(rawData, &msg.Data)
	if err != nil {
		slog.Error("unable to unmarshal data", "error", err)
		return nil, err
	}

	return &msg, nil
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

func (m MessageModel) Dequeue(tx pgx.Tx, id int64) error {
	stmt := `DELETE FROM messages
        WHERE id IN (
            SELECT id
            FROM messages
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        );`

	_, err := tx.Exec(context.Background(), stmt)
	if err != nil {
		slog.Error("unable to execute statement", "error", err)
		return err
	}

	return nil
}

func (m MessageModel) Notify() error {
	conn, err := m.Pool.Acquire(context.Background())
	if err != nil {
		slog.Error("unable to acquire connection", "error", err)
		return err
	}
	defer conn.Release()

	_, err = conn.Exec(context.Background(), "NOTIFY message_channel, 'New message';")
	if err != nil {
		slog.Error("unable to execute notification statement", "error", err)
		return err
	}

	return nil
}

func (m MessageModel) Listen(pool *pgxpool.Pool, ch chan<- pgconn.Notification) {
	for {
		conn, err := pool.Acquire(context.Background())
		if err != nil {
			slog.Error("unable to acquire connection", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		_, err = conn.Exec(context.Background(), "LISTEN message_channel;")
		if err != nil {
			slog.Error("unable to listen", "error", err)
			conn.Release()
			continue
		}

		for {
			notification, err := conn.Conn().WaitForNotification(context.Background())
			if err != nil {
				slog.Error("unable to receive notification", "error", err)
				break
			}
			ch <- *notification
		}

		conn.Release()
	}
}
