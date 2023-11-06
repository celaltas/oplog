package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	_ "sync"
	"syscall"
	"time"

	"github.com/celal/oplog-migration/internal/config"
	"github.com/celal/oplog-migration/internal/core/domain"
	"github.com/celal/oplog-migration/internal/core/services"
	"github.com/celal/oplog-migration/internal/database"
	"github.com/celal/oplog-migration/internal/repositories"
)

func main() {

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	mongoConfig := config.NewMongoConfig()
	mongoClient := database.ConnectMongo(mongoConfig)
	mongoRepository := repositories.NewOplogReaderMongoRepository(mongoClient, ctx)
	oplogReader := services.NewOplogReaderService(mongoRepository)

	postgresConfig := config.NewConfigPostgres()
	postgresClient := database.ConnectPostgreSQL(postgresConfig)
	postgresRepository := repositories.NewOplogWriterPostgresRepository(postgresClient)
	oplogWriter := services.NewOplogWriterService(postgresRepository)

	collectionName := "admin.students"

	oplog, err := oplogReader.ReadOplog(collectionName)
	if err != nil {
		fmt.Println(err)
	}

	sqlStatements, err := domain.GenerateSQL(string(oplog))

	if err != nil {
		fmt.Println(err)
	}

	ctx, cancel = context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(len(sqlStatements))
	wp, err := domain.NewWorkerPool(len(sqlStatements), 0)
	if err != nil {
		panic(err)
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
