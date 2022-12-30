package indexer

import (
	"go.mongodb.org/mongo-driver/mongo"
)

type CollectionIndexes struct {
	CollName string
	Indexes  []mongo.IndexModel
}
