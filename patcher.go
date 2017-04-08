package astipatch

import "github.com/rs/xlog"

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
	SetLogger(xlog.Logger)
}

// patcherBase represents a base patcher
type patcherBase struct {
	logger xlog.Logger
}

// newPatcherBase creates a new base patcher
func newPatcherBase() *patcherBase {
	return &patcherBase{logger: xlog.NopLogger}
}

// SetLogger sets the patcher's logger
func (p *patcherBase) SetLogger(l xlog.Logger) {
	p.logger = l
}
