package main

import (
	"context"
	"github.com/hextechpal/mim/indexer"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		panic(err)
	}

	db := client.Database("mim")
	colls := []indexer.CollectionIndexes{{
		CollName: "products",
		Indexes: []mongo.IndexModel{{
			Keys:    bson.D{{"item", 1}, {"category", 1}},
			Options: &options.IndexOptions{Name: strPtr("item_1_category_1")},
		}},
	}}
	manager := indexer.NewManager(false, indexer.AUTO, colls, db)
	manager.Ensure(context.Background())
}

func strPtr(s string) *string {
	var ss string
	ss = s
	return &ss
}
