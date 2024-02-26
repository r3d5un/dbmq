package data

import (
	"log/slog"
	"time"

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

func (m MessageModel) Insert(data Data) (*Message, error) {
	slog.Error("not implemented")
	return nil, nil
}

func (m MessageModel) GetNext() (*Message, error) {
	slog.Error("not implemented")
	return nil, nil
}

func (m MessageModel) Delete(id int64) error {
	slog.Error("not implemented")
	return nil
}
