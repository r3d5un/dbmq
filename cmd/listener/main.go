package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"

	// "github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	slog.Info("starting listener...")

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

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		slog.Error("unable to create connection", "error", err)
		os.Exit(1)
	}
	defer conn.Release()

	slog.Info("listening for notifications...")
	_, err = conn.Exec(context.Background(), "LISTEN dbmq_channel")
	if err != nil {
		slog.Error("unable to listen", "error", err)
		os.Exit(1)
	}

	for {
		notification, err := conn.Conn().WaitForNotification(context.Background())
		if err != nil {
			slog.Error("unable to receive notification", "error", err)
			continue
		}

		slog.Info("received notification", "channel", notification.Channel, "payload", notification.Payload)
	}
}

func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}
