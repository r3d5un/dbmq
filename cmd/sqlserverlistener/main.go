package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

func main() {
	handler := slog.NewJSONHandler(os.Stdout, nil)
	slog.SetDefault(slog.New(handler))

	slog.Info("starting listener...")

	slog.Info("opening database connection pool...")
	db, err := openDB("sqlserver://sa:oPax9HFmjU4AV%5EAqXEeA@localhost:1433?database=master&TrustServerCertificate=true")
	if err != nil {
		slog.Error("error occurred while connecting to the database",
			"error", err,
		)
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("database connection pool established")
}

func sendMessage(db *sql.DB, message string) (rowsAffected *int64, err error) {
	tx, err := db.Begin()
	if err != nil {
		slog.Error("error occurred while starting transaction", "error", err)
		return nil, err
	}
	defer tx.Rollback()

	res, err := db.Exec(`
        BEGIN DIALOG @conversation_handle
        FROM SERVICE [RequestService]
        TO SERVICE 'ResponseService'
        ON CONTRACT [ProcessingContract]
        WITH ENCRYPTION = OFF;

        SET @message = N'Hello, World!';

        SEND ON CONVERSATION @conversation_handle
        MESSAGE TYPE [RequestMessage] (@p1);`,
		message,
	)
	if err != nil {
		slog.Error("error occurred while sending message", "error", err)
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		slog.Error("error occurred while committing transaction", "error", err)
		return nil, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		slog.Error("error occurred while getting rows affected", "error", err)
		return nil, err
	}

	return &affected, nil
}

type IncomingMessage struct {
	ConversationHandle string `json:"conversation_handle"`
	TypeName           string `json:"type_name"`
	Body               string `json:"body"`
}

func receiveMessage(db *sql.DB) (msg *IncomingMessage, err error) {
	tx, err := db.Begin()
	if err != nil {
		slog.Error("error occurred while starting transaction", "error", err)
		return nil, err
	}
	defer tx.Rollback()

	err = db.QueryRow(`
		RECEIVE TOP(1)
			@conversation_handle = conversation_handle,
			@message_type_name = message_type_name,
			@message_body = message_body
		FROM dbo.RequestQueue;

		SELECT @conversation_handle, @message_type_name, @message_body

		IF (@message_type_name = 'RequestMessage')
			BEGIN
				SET @response_message = N'Foo';
				SEND ON CONVERSATION @conversation_handle MESSAGE TYPE [ResponseMessage] (@response_message);
				END CONVERSATION  @conversation_handle;
			END
	`).Scan(
		&msg.ConversationHandle,
		&msg.TypeName,
		&msg.Body,
	)
	if err != nil {
		slog.Error("unable to get message from message broker", "error", err)
		return nil, err
	}

	return msg, err
}
func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)

	duration, err := time.ParseDuration("5m")
	if err != nil {
		return nil, err
	}
	db.SetConnMaxIdleTime(duration)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return db, nil
}
