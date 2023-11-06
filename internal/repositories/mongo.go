package repositories

import (
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type OplogReaderMongoRepository struct {
	client *mongo.Client
	ctx    context.Context
}

func NewOplogReaderMongoRepository(client *mongo.Client, ctx context.Context) *OplogReaderMongoRepository {
	return &OplogReaderMongoRepository{
		client: client,
		ctx:    ctx,
	}
}

func (s *OplogReaderMongoRepository) ReadOplog(collectionName string) ([]byte, error) {
	return s.getOplogs(collectionName)
}

func (s *OplogReaderMongoRepository) getOplogs(collectionName string) ([]byte, error) {
	db := s.client.Database("local")
	filter := bson.D{{Key: "ns", Value: "admin.students"}}
	var result []map[string]interface{}
	cursor, err := db.Collection("oplog.rs").Find(s.ctx, filter)
	if err != nil {
		return nil, err

	}
	defer cursor.Close(s.ctx)

	if err := cursor.All(s.ctx, &result); err != nil {
		return nil, err

	}

	oplogJson, err := json.Marshal(result)
	if err != nil {
		return nil, err

	}

	return oplogJson, nil
}
