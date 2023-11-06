package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

type PostgresConfig struct {
	Dsn          string
	MaxOpenConns int
	MaxIdleConns int
	MaxIdleTime  string
}

func NewConfigPostgres() PostgresConfig {

	if err := godotenv.Load(); err != nil {
		log.Panic("Error loading .env file", err)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("POSTGRES_HOST"),
		os.Getenv("POSTGRES_PORT"),
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_DATABASE"))

	return PostgresConfig{
		Dsn:          dsn,
		MaxOpenConns: 10,
		MaxIdleConns: 5,
		MaxIdleTime:  "10s",
	}
}
