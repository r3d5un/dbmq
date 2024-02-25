package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5"
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

	for {
		slog.Info("acquiring connection...")
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

		notification, err := conn.Conn().WaitForNotification(context.Background())
		if err != nil {
			slog.Error("unable to receive notification", "error", err)
			continue
		}

		slog.Info("received notification", "channel", notification.Channel, "payload", notification.Payload)

		err = consumeMessages(conn)
		if err != nil {
			slog.Error("unable to consume messages", "error", err)
			continue
		}

		slog.Info("waiting for next notification...")
	}
}

func consumeMessages(conn *pgxpool.Conn) error {
	newMessage := true

	stmt := `SELECT id, data, created_at
FROM messages
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT 1;`

	delStmt := `DELETE FROM messages
WHERE id IN (
    SELECT id
    FROM messages
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
);`

	slog.Info("looping through messages...")
	for newMessage {
		slog.Info("processing message...")

		slog.Info("beginning transaction...")
		tx, err := conn.Begin(context.Background())
		if err != nil {
			slog.Error("unable to begin transaction", "error", err)
			return err
		}

		slog.Info("executing SELECT statement...")
		res, err := tx.Exec(context.Background(), stmt)
		if err != nil {
			switch err {
			case sql.ErrNoRows:
				slog.Info("no new messages", "error", err)
				newMessage = false
				break
			case pgx.ErrNoRows:
				slog.Info("no new messages", "error", err)
				newMessage = false
				break
			default:
				slog.Error("unable to execute statement", "error", err)
				break
			}
		}
		slog.Info("select query executed", "rows_affected", res.RowsAffected())
		if res.RowsAffected() == 0 {
			newMessage = false
			return nil
		}

		slog.Info("executing DELETE statement...")
		_, err = tx.Exec(context.Background(), delStmt)
		if err != nil {
			slog.Error("unable to execute statement", "error", err)
			tx.Rollback(context.Background())
			continue
		}

		slog.Info("processed message")
		err = tx.Commit(context.Background())
		if err != nil {
			slog.Error("unable to commit transaction", "error", err)
			return err
		}
	}

	return nil
}
