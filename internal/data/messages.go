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

func (m MessageModel) NewMessage(data Data) (*Message, error) {
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

	err = m.Pool.QueryRow(context.Background(), stmt, dataJSON).Scan(
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
			return nil, err
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

func (m MessageModel) ConsumeAll(ch chan<- Message) error {
	tx, err := m.Pool.Begin(context.Background())
	if err != nil {
		slog.Error("unable to begin transaction", "error", err)
		return err
	}
	defer tx.Rollback(context.Background())

	for {
		tx, err = m.Pool.Begin(context.Background())

		slog.Info("getting next message...")
		msg, err := m.GetNext(tx)
		if err != nil {
			switch err {
			case pgx.ErrNoRows:
				slog.Error("no rows found", "error", err)
				return err
			default:
				slog.Error("unable to get next message", "error", err)
				return err
			}
		}

		if msg == nil {
			slog.Info("no new messages")
			break
		}

		slog.Info("processing message...")
		ch <- *msg

		slog.Info("dequeuing message...")
		err = m.Dequeue(tx, *msg.ID)
		if err != nil {
			slog.Error("unable to dequeue message", "error", err)
			tx.Rollback(context.Background())
			return err
		}

		slog.Info("committing transaction...")
		err = tx.Commit(context.Background())
		if err != nil {
			slog.Error("unable to commit transaction", "error", err)
			return err
		}
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

func (m MessageModel) Listen(ch chan<- pgconn.Notification, done <-chan bool) {
	for {
		slog.Info("acquiring connection...")
		conn, err := m.Pool.Acquire(context.Background())
		if err != nil {
			slog.Error("unable to acquire connection", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		slog.Info("executing LISTEN statement...")
		_, err = conn.Exec(context.Background(), "LISTEN message_channel;")
		if err != nil {
			slog.Error("unable to listen", "error", err)
			conn.Release()
			continue
		}

		for {
			select {
			case <-done:
				slog.Info("received done signal, stopping listener.")
				return
			default:
				if notification, err := conn.Conn().WaitForNotification(context.Background()); err != nil {
					slog.Info("Unable to receive notification", "error", err)
					break
				} else {
					slog.Info("received notification")
					ch <- *notification
				}
			}
		}
	}
}
