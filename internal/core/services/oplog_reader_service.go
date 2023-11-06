package services

import "github.com/celal/oplog-migration/internal/core/ports"


type OplogReaderService struct {
	repo ports.OplogReader
}




func NewOplogReaderService(repo ports.OplogReader) *OplogReaderService {
	return &OplogReaderService{repo: repo}
}

func (s *OplogReaderService) ReadOplog(collectionName string) ([]byte, error) {
	return s.repo.ReadOplog(collectionName)

}