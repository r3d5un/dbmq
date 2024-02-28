package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/r3d5un/dbmq/internal/data"
)

func main() {
	handler := slog.NewJSONHandler(os.Stdout, nil)
	slog.SetDefault(slog.New(handler))

	slog.Info("starting notifier")

	config, err := pgxpool.ParseConfig("postgresql://postgres:postgres@localhost:5432?database=dbmq")
	if err != nil {
		slog.Error("unable to parse config", "error", err)
		os.Exit(1)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		slog.Error("unable to create connection pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	slog.Info("connection pool established")

	slog.Info("creating models...")
	models := data.NewModels(pool)

	slog.Info("creating message...")
	data := data.Data{
		MessageBody: "hello world",
	}

	for {
		slog.Info("sending message")
		msg, err := models.Messages.NewMessage(data)
		if err != nil {
			slog.Error("unable to send message", "error", err)
			break
		}

		slog.Info("notifying")
		models.Messages.Notify()

		slog.Info("message sent", "id", *msg.ID)

		time.Sleep(5 * time.Second)
	}

	os.Exit(0)
}
