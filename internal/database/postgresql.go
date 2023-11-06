package database

import (
	"context"
	"database/sql"
	"time"

	"github.com/celal/oplog-migration/internal/config"
	_ "github.com/lib/pq"
)

func ConnectPostgreSQL(cfg config.PostgresConfig) *sql.DB {
	db, err := sql.Open("postgres", cfg.Dsn)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	duration, err := time.ParseDuration(cfg.MaxIdleTime)
	if err != nil {
		panic(err)
	}
	db.SetConnMaxIdleTime(duration)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		panic(err)
	}
	return db

}