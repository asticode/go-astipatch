package astipatch

import "github.com/rs/xlog"

// Storer represents an object capable of storing patch's state
type Storer interface {
	DeleteLastBatch() error
	Delta([]string) ([]string, error)
	Init() error
	InsertBatch([]string) error
	LastBatch() ([]string, error)
	SetLogger(xlog.Logger)
}

// storerBase represents a base storer
type storerBase struct {
	logger xlog.Logger
}

// newStorerBase creates a new base storer
func newStorerBase() *storerBase {
	return &storerBase{logger: xlog.NopLogger}
}

// SetLogger sets the storer's logger
func (s *storerBase) SetLogger(l xlog.Logger) {
	s.logger = l
}
