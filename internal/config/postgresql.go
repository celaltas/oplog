package config

import (
	"os"
	"github.com/joho/godotenv"
)

type PostgresConfig struct {
	Dsn          string
	MaxOpenConns int
	MaxIdleConns int
	MaxIdleTime  string
}


func NewConfigPostgres() PostgresConfig {
	
	if err:= godotenv.Load();err!=nil{
		panic(err)
	}


	dsn:= "host="+os.Getenv("POSTGRES_HOST")+
		" port="+os.Getenv("POSTGRES_PORT")+
		" user="+os.Getenv("POSTGRES_USER")+
		" password="+os.Getenv("POSTGRES_PASSWORD")+
		" dbname="+os.Getenv("POSTGRES_DATABASE")+
		" sslmode=disable"

	return PostgresConfig{
		Dsn:          dsn,
		MaxOpenConns: 10,
		MaxIdleConns: 5,
		MaxIdleTime:  "10s",
	}
}