package astipatch

// Constants
const (
	rollbackSuffix = "_rollback"
)

// Patcher represents an object capable of patching
type Patcher interface {
	Init() error
	Load(c Configuration) error
	Patch() error
	Rollback() error
}
