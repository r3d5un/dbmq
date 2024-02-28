package main

import (
	"context"
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
	handler := slog.NewJSONHandler(os.Stdout, nil)
	slog.SetDefault(slog.New(handler))

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
	notificationChannel := make(chan pgconn.Notification, 10)
	messageChannel := make(chan data.Message, 10)
	done := make(chan bool)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("creating models...")
	models := data.NewModels(pool)

	slog.Info("listening for notifications...")
	go models.Messages.Listen(notificationChannel, done)

	go func() {
		select {
		case <-done:
			slog.Info("stopping listener")
			return
		case <-signals:
			slog.Info("stopping listener")
			done <- true
			return
		case <-messageChannel:
			slog.Info("processing message")
			for m := range messageChannel {
				slog.Info("message processed", "id", m.ID, "data", m.Data, "created_at", m.CreatedAt)
			}
		}
	}()

	go func() {
		for {
			slog.Info("listening for messages...")
			select {
			case notification := <-notificationChannel:
				slog.Info(
					"received notification",
					"channel", notification.Channel,
					"payload", notification.Payload,
				)

				slog.Info("retrieving messages")
				err = models.Messages.ConsumeAll(messageChannel)
				if err != nil {
					switch err {
					case pgx.ErrNoRows:
						slog.Info("no more messages", "error", err)
						continue
					default:
						slog.Error("unable to retrieve messages", "error", err)
						continue
					}
				}

				// slog.Info("retrieving message")
				// tx, err := pool.Begin(context.Background())
				// if err != nil {
				// 	slog.Error("unable to begin transaction", "error", err)
				// 	continue
				// }
				//
				// d, err := models.Messages.GetNext(tx)
				// if err != nil {
				// 	slog.Error("unable to retrieve message", "error", err)
				// 	continue
				// }
				// slog.Info(
				// 	"message retrieved",
				// 	"id", *d.ID, "data", *d.Data, "created_at", *d.CreatedAt,
				// )
				//
				// slog.Info("dequeueing message")
				// err = models.Messages.Dequeue(tx, *d.ID)
				// if err != nil {
				// 	slog.Error("unable to dequeue message", "error", err)
				// 	continue
				// }
				// slog.Info("message dequeued")
				//
				// err = tx.Commit(context.Background())
				// if err != nil {
				// 	slog.Error("unable to commit transaction", "error", err)
				// 	continue
				// }
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
