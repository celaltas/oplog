package repositories

import (
	"context"
	"database/sql"
)

type OplogWriterPostgresRepository struct {
	db *sql.DB
}

func NewOplogWriterPostgresRepository(db *sql.DB) *OplogWriterPostgresRepository {
	return &OplogWriterPostgresRepository{
		db: db,
	}
}

func (r *OplogWriterPostgresRepository) WriteOplog(ctx context.Context, sql string) error {
	_, err := r.db.ExecContext(ctx, sql)
	return err
}
