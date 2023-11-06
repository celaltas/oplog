package services

import (
	"context"

	"github.com/celal/oplog-migration/internal/core/ports"
)


type OplogWriterService struct {
	repo ports.OplogWriter
}




func NewOplogWriterService(repo ports.OplogWriter) *OplogWriterService {
	return &OplogWriterService{repo: repo}
}

func (s *OplogWriterService) WriteOplog(ctx context.Context, sql string) error{
	return s.repo.WriteOplog(ctx,sql)
}