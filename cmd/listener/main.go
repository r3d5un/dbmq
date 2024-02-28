package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/r3d5un/dbmq/internal/data"
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

	slog.Info("opening channels...")
	notificationChannel := make(chan pgconn.Notification)
	done := make(chan bool)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("creating models...")
	models := data.NewModels(pool)

	slog.Info("listening for notifications...")
	go models.Messages.Listen(notificationChannel, done)

	slog.Info("listening for messages...")
	go func() {
		for {
			select {
			case notification := <-notificationChannel:
				slog.Info(
					"received notification",
					"channel", notification.Channel,
					"payload", notification.Payload,
				)

				slog.Info("retrieving message")
				tx, err := pool.Begin(context.Background())
				if err != nil {
					slog.Error("unable to begin transaction", "error", err)
					continue
				}

				d, err := models.Messages.GetNext(tx)
				if err != nil {
					slog.Error("unable to retrieve message", "error", err)
					continue
				}
				slog.Info(
					"message retrieved",
					"id", *d.ID, "data", *d.Data, "created_at", *d.CreatedAt,
				)

				slog.Info("dequeueing message")
				err = models.Messages.Dequeue(tx, *d.ID)
				if err != nil {
					slog.Error("unable to dequeue message", "error", err)
					continue
				}
				slog.Info("message dequeued")

				err = tx.Commit(context.Background())
				if err != nil {
					slog.Error("unable to commit transaction", "error", err)
					continue
				}
			case <-signals:
				slog.Info("stopping listener")
				done <- true
				return
			}
		}
	}()

	<-done
	slog.Info("stopping listening process")
	os.Exit(0)
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
