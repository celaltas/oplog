package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	log "github.com/sirupsen/logrus"
	"github.com/celal/oplog-migration/internal/config"
	"github.com/celal/oplog-migration/internal/core/domain"
	"github.com/celal/oplog-migration/internal/core/services"
	"github.com/celal/oplog-migration/internal/database"
	"github.com/celal/oplog-migration/internal/repositories"
)


func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
  
}

func main() {


	log.Info("Oplog starting...")


	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	mongoConfig := config.NewMongoConfig()
	log.Info("Connecting mongo db...")
	mongoClient := database.ConnectMongo(mongoConfig)
	mongoRepository := repositories.NewOplogReaderMongoRepository(mongoClient, ctx)
	oplogReader := services.NewOplogReaderService(mongoRepository)

	postgresConfig := config.NewConfigPostgres()
	log.Info("Connecting postgresql db...")
	postgresClient := database.ConnectPostgreSQL(postgresConfig)
	postgresRepository := repositories.NewOplogWriterPostgresRepository(postgresClient)
	oplogWriter := services.NewOplogWriterService(postgresRepository)

	collectionName := "admin.students"

	oplog, err := oplogReader.ReadOplog(collectionName)
	if err != nil {
		log.Fatal("error when reading oplog:", err)
	}

	sqlStatements, err := domain.GenerateSQL(string(oplog))
	if err != nil {
		log.Fatal("error when transforming oplog to sql statements:", err)
	}

	ctx, cancel = context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(len(sqlStatements))
	wp, err := domain.NewWorkerPool(len(sqlStatements), 0)
	if err != nil {
		log.Fatal("error when creating worker pool:", err)
	}

	var tasks []domain.TaskHandler
	for _, sql := range sqlStatements {
		tasks = append(tasks, domain.NewTask(oplogWriter.WriteOplog, sql, wg, ctx))
	}

	go func() {
		<-sig
		cancel()
		wp.Stop()
	}()

	wp.SubmitNonBlocking(tasks)
	wp.Start()
	wp.Err()
	wg.Wait()

}
