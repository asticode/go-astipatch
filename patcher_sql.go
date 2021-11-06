package astipatch

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/asticode/go-astikit"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
)

// patcherSQL represents a SQL patcher
type patcherSQL struct {
	conn             *sqlx.DB
	l                astikit.SeverityLogger
	patches          map[string]*PatchSQL // Indexed by name
	sortedPatchNames []string
	storer           Storer
}

type PatchSQL []PatchSQLTransaction

type PatchSQLTransaction struct {
	Queries   []string `yaml:"queries"`
	Rollbacks []string `yaml:"rollbacks"`
}

// NewPatcherSQL creates a new SQL patcher
func NewPatcherSQL(conn *sqlx.DB, s Storer, l astikit.StdLogger) Patcher {
	return &patcherSQL{
		conn:    conn,
		l:       astikit.AdaptStdLogger(l),
		patches: make(map[string]*PatchSQL),
		storer:  s,
	}
}

// Init implements the Patcher interface
func (p *patcherSQL) Init() error {
	return p.storer.Init()
}

// Load loads the patches
func (p *patcherSQL) Load(c Configuration) (err error) {
	// Clean patches directory path
	dirPath := filepath.Clean(c.PatchesDirectoryPath)

	// Empty directory path
	if dirPath == "" {
		return
	}

	// Log
	p.l.Debug("astipatch: loading patches")

	// Walk patches directory path
	if err = filepath.Walk(dirPath, func(path string, info os.FileInfo, _ error) (err error) {
		// Skip directories
		if info.IsDir() {
			return
		}

		// Only process .yml files
		if filepath.Ext(path) != ".yml" {
			return
		}

		// Open file
		var f *os.File
		if f, err = os.Open(path); err != nil {
			err = fmt.Errorf("astipatch: opening %s failed: %w", path, err)
			return
		}
		defer f.Close()

		// Unmarshal
		var y PatchSQL
		if err = yaml.NewDecoder(f).Decode(&y); err != nil {
			err = fmt.Errorf("astipatch: unmarshaling failed: %w", err)
			return
		}

		// Get name
		name := strings.TrimSuffix(strings.TrimPrefix(path, dirPath+string(os.PathSeparator)), filepath.Ext(path))

		// Add patch
		if _, ok := p.patches[name]; !ok {
			p.patches[name] = &y
			p.sortedPatchNames = append(p.sortedPatchNames, name)
		}
		return
	}); err != nil {
		return
	}

	// Log
	p.l.Debugf("astipatch: %d loaded patch(es)", len(p.patches))

	// Sort patch names
	sort.Strings(p.sortedPatchNames)
	return
}

// Patch implements the Patcher interface
func (p *patcherSQL) Patch() (err error) {
	// Get patches to run
	var patches []string
	if patches, err = p.storer.Delta(p.sortedPatchNames); err != nil {
		err = fmt.Errorf("astipatch: getting delta failed: %w", err)
		return
	}

	// Log
	p.l.Debugf("astipatch: %d patch(es) to run", len(patches))

	// No patches to run
	if len(patches) == 0 {
		return
	}

	// Patch
	if err = p.patch(patches); err != nil {
		return
	}

	// Insert batch
	if err = p.storer.InsertBatch(patches); err != nil {
		err = fmt.Errorf("astipatch: inserting batch failed: %w", err)
		return
	}
	return
}

func (p *patcherSQL) patch(patches []string) (err error) {
	// In case of error, we need to exec rollbacks of transactions that were successfully executed
	var transactions []PatchSQLTransaction
	defer func(err *error, transactions *[]PatchSQLTransaction) {
		// No error
		if *err == nil {
			return
		}

		// Loop through transactions in reverse order
		for idx := len(*transactions) - 1; idx >= 0; idx-- {
			// Exec
			if e := p.exec((*transactions)[idx].Rollbacks); e != nil {
				p.l.Error(fmt.Errorf("astipatch: executing failed: %w", e))
				return
			}
		}
	}(&err, &transactions)

	// Loop through patches
	for _, patch := range patches {
		// Log
		p.l.Debugf("astipatch: executing queries of patch %s", patch)

		// Loop through transactions
		for idx, transaction := range *p.patches[patch] {
			// Exec
			if err = p.exec(transaction.Queries); err != nil {
				err = fmt.Errorf("astipatch: executing queries of transaction %d of patch %s failed: %w", idx, patch, err)
				return
			}

			// Append transaction
			transactions = append(transactions, transaction)
		}
	}
	return
}

// Rollback implements the Patcher interface
func (p *patcherSQL) Rollback() (err error) {
	// Get patches to rollback
	var patches []string
	if patches, err = p.storer.LastBatch(); err != nil {
		return
	}

	// Log
	p.l.Debugf("astipatch: %d patch(es) to rollback", len(patches))

	// No patches to rollback
	if len(patches) == 0 {
		return
	}

	// Rollback
	if err = p.rollback(patches); err != nil {
		err = fmt.Errorf("astipatch: rolling back failed: %w", err)
		return
	}

	// Delete last batch
	if err = p.storer.DeleteLastBatch(); err != nil {
		err = fmt.Errorf("astipatch: deleting last batch failed: %w", err)
		return
	}
	return
}

func (p *patcherSQL) rollback(patches []string) (err error) {
	// Loop through patches in reverse order
	for idxPatch := len(patches) - 1; idxPatch >= 0; idxPatch-- {
		// Log
		p.l.Debugf("astipatch: executing rollbacks of patch %s", patches[idxPatch])

		// Loop through transactions in reverse order
		for idxTransaction := len(*p.patches[patches[idxPatch]]) - 1; idxTransaction >= 0; idxTransaction-- {
			// Exec
			if err = p.exec((*p.patches[patches[idxPatch]])[idxTransaction].Rollbacks); err != nil {
				err = fmt.Errorf("astipatch: executing rollbacks of transaction %d of patch %s failed: %w", idxTransaction, patches[idxPatch], err)
				return
			}
		}
	}
	return
}

func (p *patcherSQL) exec(queries []string) (err error) {
	// Start transaction
	var tx *sqlx.Tx
	if tx, err = p.conn.Beginx(); err != nil {
		err = fmt.Errorf("astipatch: starting transaction failed: %w", err)
		return
	}

	// Commit/Rollback
	defer func(err *error) {
		if *err != nil {
			if e := tx.Rollback(); e != nil {
				p.l.Error(fmt.Errorf("astipatch: rolling back transaction failed: %w", e))
			}
		} else {
			if e := tx.Commit(); e != nil {
				p.l.Error(fmt.Errorf("astipatch: committing transaction failed: %w", e))
			}
		}
	}(&err)

	// Loop through queries
	for _, query := range queries {
		// Exec
		if _, err = tx.Exec(query); err != nil {
			err = fmt.Errorf("astipatch: executing %s failed: %w", query, err)
			return
		}
	}
	return
}
