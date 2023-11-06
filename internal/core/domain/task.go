package domain

import (
	"context"
	"sync"
)

type Task struct {
	executeFunc func(context.Context, string) error
	wg          *sync.WaitGroup
	sql         string
	ctx         context.Context
}

func NewTask(executeFunc func(context.Context, string) error, sql string, wg *sync.WaitGroup, ctx context.Context) *Task {
	return &Task{
		executeFunc: executeFunc,
		sql:         sql,
		wg:          wg,
		ctx:         ctx,
	}
}

func (t *Task) Execute() error {
	if t.wg != nil {
		defer t.wg.Done()
	}
	if t.executeFunc != nil {
		return t.executeFunc(t.ctx, t.sql)
	}
	return nil
}

func (t *Task) OnFailure(e error, errChan chan<- error) {
	errChan <- e
}

