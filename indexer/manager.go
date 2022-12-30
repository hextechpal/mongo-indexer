package indexer

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
)

type Mode int

const (
	AUTO Mode = iota
	MANUAL
)

type Manager struct {
	mode        Mode
	collections []CollectionIndexes
	db          *mongo.Database
	logger      *zerolog.Logger
}

func NewManager(debug bool, mode Mode, collections []CollectionIndexes, db *mongo.Database) *Manager {
	return &Manager{
		mode:        mode,
		collections: collections,
		db:          db,
		logger:      initLogger(debug, db.Name()),
	}
}

func initLogger(debug bool, dbName string) *zerolog.Logger {
	logLevel := zerolog.InfoLevel
	if debug {
		logLevel = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(logLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	logger := zerolog.
		New(os.Stderr).
		With().
		Timestamp().
		Str("db", dbName).
		Logger().
		Output(zerolog.ConsoleWriter{Out: os.Stderr})
	return &logger
}

func (m *Manager) Ensure(ctx context.Context) error {
	for _, col := range m.collections {
		cm, err := NewCollManager(ctx, col, m.db, m.logger)
		if err != nil {
			return err
		}
		cm.ensure(ctx, col.Indexes)
	}
	return nil
}
