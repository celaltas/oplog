package domain

import (
	"fmt"
	"sync"
	"time"
)

var ErrNoWorkers = fmt.Errorf("attempting to create worker pool with less than 1 worker")
var ErrNegativeChannelSize = fmt.Errorf("attempting to create worker pool with a negative channel size")

type TaskHandler interface {
	Execute() error
	OnFailure(error, chan<- error)
}

type Pooler interface {
	Start()
	Stop()
	Submit(TaskHandler)
}

type WorkerPool struct {
	numWorkers int
	tasks      chan TaskHandler
	start      sync.Once
	stop       sync.Once
	quit       chan struct{}
	errChan    chan error
}

func NewWorkerPool(numWorkers int, channelSize int) (*WorkerPool, error) {
	if numWorkers < 1 {
		return nil, ErrNoWorkers
	}
	if channelSize < 0 {
		return nil, ErrNegativeChannelSize
	}

	tasks := make(chan TaskHandler, channelSize)
	return &WorkerPool{
		numWorkers: numWorkers,
		tasks:      tasks,
		start:      sync.Once{},
		stop:       sync.Once{},
		quit:       make(chan struct{}),
		errChan:    make(chan error),
	}, nil
}

func (wp *WorkerPool) Start() {
	wp.start.Do(func() {
		fmt.Println("starting worker pool")
		wp.startWorkers()
	})
}

func (wp *WorkerPool) Stop() {
	wp.stop.Do(func() {
		fmt.Println("stopping worker pool")
		close(wp.quit)

	})
}

func (wp *WorkerPool) Err() {
	select {
		case err := <-wp.errChan:
			fmt.Println("error occurred: ", err)
		case <-wp.quit:
	}

}

func (wp *WorkerPool) Submit(task TaskHandler) {
	select {
	case wp.tasks <- task:
	case <-wp.quit:
	}
}

func (wp *WorkerPool) SubmitNonBlocking(tasks []TaskHandler) {
	for _, task := range tasks {
		go wp.Submit(task)
	}
}

func (wp *WorkerPool) startWorkers() {
	for i := 0; i < wp.numWorkers; i++ {
		go func(workerNum int) {
			fmt.Printf("starting worker %d\n", workerNum)
			for {
				select {
				case <-wp.quit:
					fmt.Printf("stopping worker %d with quit channel\n", workerNum)
				case task, ok := <-wp.tasks:
					if !ok {
						fmt.Printf("stopping worker %d with closed tasks channel\n", workerNum)
						return
					}
					time.Sleep(1*time.Second)
					if err := task.Execute(); err != nil {
						task.OnFailure(err, wp.errChan)
					}

				}
			}
		}(i)
	}
}
