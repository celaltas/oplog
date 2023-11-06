package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
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
		panic(err)
	}

	dsn := "mongodb://" +
		os.Getenv("MONGODB_USER") + ":" +
		os.Getenv("MONGODB_PASSWORD") + "@" +
		os.Getenv("MONGODB_HOST") + ":" +
		os.Getenv("MONGODB_PORT") + "/" +
		os.Getenv("MONGODB_DATABASE") + "?authSource=admin&replicaSet=dbrs&directConnection=true"
	fmt.Println("dsn:", dsn)
	return &MongoConfig{
		Dsn:              dsn,
		ConnectTimeoutMS: 3000,
		TimeoutMS:        3000,
		MaxPoolSize:      10,
		MaxIdleTimeMS:    3000,
	}
}
