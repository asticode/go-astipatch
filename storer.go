package astipatch

// Storer represents an object capable of storing patch's state
type Storer interface {
	DeleteLastBatch() error
	Delta([]string) ([]string, error)
	Init() error
	InsertBatch([]string) error
	LastBatch() ([]string, error)
}
