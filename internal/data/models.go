package data

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrRecordNotFound = errors.New("record not found")
)

type Models struct {
	Messages MessageModel
}

func NewModels(pool *pgxpool.Pool) Models {
	return Models{
		Messages: MessageModel{Pool: pool},
	}
}
