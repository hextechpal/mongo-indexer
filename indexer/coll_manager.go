package indexer

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
)

type indexStatus int

const (
	NOOP indexStatus = iota
	REBUILD
	CREATE

	idxName = "name"
	idxKey  = "key"
)

type CollManager struct {
	coll     *mongo.Collection
	eIndexes map[string]*bson.D
	logger   *zerolog.Logger
}

func NewCollManager(ctx context.Context, ci CollectionIndexes, db *mongo.Database, logger *zerolog.Logger) (*CollManager, error) {
	nLogger := logger.With().Str("coll", ci.CollName).Logger()
	coll := db.Collection(ci.CollName)
	info, err := existingIndexes(ctx, coll)
	if err != nil {
		return nil, err
	}
	nLogger.Info().Msgf("existing indexes=%v", info)
	return &CollManager{coll: coll, eIndexes: info, logger: &nLogger}, nil
}

func (cm *CollManager) ensure(ctx context.Context, idxs []mongo.IndexModel) error {
	for _, idx := range idxs {
		status := cm.indexStatus(idx)
		cm.logger.Info().Msgf("calculated index status=%v, idx=%v", status, idx)

		switch status {
		case REBUILD:
			// TODO : Rebuild existing index
		case CREATE:
			_, err := cm.coll.Indexes().CreateOne(ctx, idx)
			if err != nil {
				return err
			}
		case NOOP:
			// Doing nothing
		}
	}
	return nil
}

func (cm *CollManager) indexStatus(idx mongo.IndexModel) indexStatus {
	nIdx := cm.findByName(idx)
	sIdx := cm.findBySequence(idx)

	if nIdx != nil && sIdx != nil && nIdx == sIdx {
		return NOOP
	} else if nIdx != nil && sIdx == nil {
		cm.logger.Warn().Msgf("index exists with name bit not for fields=%v\n. This represents editing of existing index", idx.Keys)
		return REBUILD
	} else if sIdx != nil && nIdx == nil {
		cm.logger.Warn().Msgf("index exists with diff name for fields=%v", idx.Keys)
		return NOOP
	} else if sIdx != nil && nIdx != nil {
		cm.logger.Error().Msgf("two diff index exists one for name and one for fields. This is redundant please correct your configuration")
		return NOOP
	} else {
		cm.logger.Info().Msgf("no index found with name or fields=%v", idx.Keys)
		return CREATE
	}

}

// findByName: Find an existing index by name
func (cm *CollManager) findByName(idx mongo.IndexModel) *bson.D {
	if idx.Options != nil && idx.Options.Name != nil {
		if eidx, ok := cm.eIndexes[*(idx.Options.Name)]; ok {
			cm.logger.Info().Msgf("existing index found with name=%v", *(idx.Options.Name))
			return eidx
		}
		return nil
	}
	return nil
}

// findBySequence: Finds an existing index by sequence
func (cm *CollManager) findBySequence(idx mongo.IndexModel) *bson.D {
	for _, eidx := range cm.eIndexes {
		fields, _ := pick(*eidx, idxKey)
		if match(fields.(bson.D), idx.Keys.(bson.D)) {
			cm.logger.Info().Msgf("existing index found for seq with fields=%v", fields)
			return eidx
		}
	}
	return nil
}

// existingIndexes: Reads existing index for the collections for the session
func existingIndexes(ctx context.Context, mcoll *mongo.Collection) (map[string]*bson.D, error) {
	idxMap := make(map[string]*bson.D)
	iv, err := mcoll.Indexes().List(ctx)
	if err != nil {
		return idxMap, err
	}

	for iv.Next(ctx) {
		var idx bson.D
		_ = iv.Decode(&idx)
		idxName, _ := pick(idx, idxName)
		idxMap[idxName.(string)] = &idx
	}
	return idxMap, nil
}

// pick : pick a value form bson.D based on the key provided
func pick(doc bson.D, key string) (any, error) {
	for _, val := range doc {
		if val.Key == key {
			return val.Value, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("not found key=%s", key))
}

// match: Compare to bson.D records and compare if they are equal
func match(fields, keys bson.D) bool {
	if len(fields) != len(keys) {
		return false
	}

	for i := 0; i < len(fields); i++ {
		eel := fields[i]
		nel := keys[i]
		veq, err := areEqual(eel.Value, nel.Value)
		if err != nil {
			return false
		}

		if eel.Key != nel.Key || !veq {
			return false
		}
	}

	return true
}

func areEqual(x, y interface{}) (bool, error) {
	xv := reflect.ValueOf(x)
	yv := reflect.ValueOf(y)
	if yv.Type().ConvertibleTo(xv.Type()) {
		return xv.Interface() == yv.Convert(xv.Type()).Interface(), nil
	} else {
		return false, errors.New("types are mismatched")
	}
}
