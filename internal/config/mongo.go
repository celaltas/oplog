package config

import (
	"os"
	"fmt"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

type MongoConfig struct {
	Dsn              string
	ConnectTimeoutMS int
	TimeoutMS        int
	MaxPoolSize      int
	MaxIdleTimeMS    int
}

func NewMongoConfig() *MongoConfig {
	if err := godotenv.Load(); err != nil {
		log.Panic("Error loading .env file", err)
	}

	dsn := fmt.Sprintf("mongodb://%s:%s@%s:%s/%s?authSource=admin&replicaSet=dbrs&directConnection=true",
	os.Getenv("MONGODB_USER"),
	os.Getenv("MONGODB_PASSWORD"),
	os.Getenv("MONGODB_HOST"),
	os.Getenv("MONGODB_PORT"),
	os.Getenv("MONGODB_DATABASE"))
	return &MongoConfig{
		Dsn:              dsn,
		ConnectTimeoutMS: 3000,
		TimeoutMS:        3000,
		MaxPoolSize:      10,
		MaxIdleTimeMS:    3000,
	}
}
