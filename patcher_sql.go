package astipatch

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/asticode/go-astikit"
	"github.com/jmoiron/sqlx"
)

// Vars
var (
	sqlQuerySeparator = []byte(";")
)

// patcherSQL represents a SQL patcher
type patcherSQL struct {
	conn         *sqlx.DB
	l            astikit.SeverityLogger
	patches      map[string]*patchSQL // Indexed by name
	patchesNames []string
	storer       Storer
}

// patchSQL represents a SQL patch
type patchSQL struct {
	queries   [][]byte
	rollbacks [][]byte
}

// NewPatcherSQL creates a new SQL patcher
func NewPatcherSQL(conn *sqlx.DB, s Storer, l astikit.StdLogger) Patcher {
	return &patcherSQL{
		conn:         conn,
		l:            astikit.AdaptStdLogger(l),
		patches:      make(map[string]*patchSQL),
		patchesNames: []string{},
		storer:       s,
	}
}

// Init implements the Patcher interface
func (p *patcherSQL) Init() error {
	return p.storer.Init()
}

// Load loads the patches
func (p *patcherSQL) Load(c Configuration) (err error) {
	p.l.Debug("Loading patches")
	if c.PatchesDirectoryPath != "" {
		p.l.Debugf("Patches directory is %s", c.PatchesDirectoryPath)
		if err = filepath.Walk(c.PatchesDirectoryPath, func(path string, info os.FileInfo, _ error) (err error) {
			// Log
			p.l.Debugf("Processing %s", path)

			// Skip directories
			if info.IsDir() {
				return
			}

			// Skip none .sql files
			if filepath.Ext(path) != ".sql" {
				p.l.Debugf("Skipping non .sql file %s", path)
				return
			}

			// Retrieve name and whether it's a rollback
			var name = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
			var rollback bool
			if strings.HasSuffix(name, rollbackSuffix) {
				rollback = true
				name = strings.TrimSuffix(name, rollbackSuffix)
			}

			// Read file
			var b []byte
			if b, err = ioutil.ReadFile(path); err != nil {
				return
			}

			// Split on query separator and clean queries
			var items, queries = bytes.Split(b, sqlQuerySeparator), [][]byte{}
			for _, item := range items {
				item = bytes.TrimSpace(item)
				if len(item) > 0 {
					queries = append(queries, item)
				}
			}

			// No queries to add
			if len(queries) == 0 {
				p.l.Debug("No queries to add")
				return
			}

			// Add/update patch
			if _, ok := p.patches[name]; !ok {
				p.patches[name] = &patchSQL{}
			}
			if len(p.patches[name].queries) == 0 && !rollback {
				p.patchesNames = append(p.patchesNames, name)
			}
			if rollback {
				p.patches[name].rollbacks = append(p.patches[name].rollbacks, queries...)
				p.l.Debugf("Adding %d rollback(s) to patch %s", len(queries), name)
			} else {
				p.patches[name].queries = append(p.patches[name].queries, queries...)
				p.l.Debugf("Adding %d querie(s) to patch %s", len(queries), name)
			}
			return
		}); err != nil {
			return
		}
	}
	return
}

// Patch implements the Patcher interface
func (p *patcherSQL) Patch() (err error) {
	// Get patches to run
	var patches []string
	if patches, err = p.storer.Delta(p.patchesNames); err != nil {
		return
	}

	// No patches to run
	if len(patches) == 0 {
		p.l.Debug("No patches to run")
		return
	}

	// Patch
	if err = p.patch(patches); err != nil {
		return
	}

	// Insert batch
	p.l.Debug("Inserting batch")
	if err = p.storer.InsertBatch(patches); err != nil {
		return
	}
	return
}

func (p *patcherSQL) patch(patches []string) (err error) {
	// Start transaction
	var tx *sqlx.Tx
	if tx, err = p.conn.Beginx(); err != nil {
		return
	}
	p.l.Debug("Beginning transaction")

	// Commit/Rollback
	defer func(err *error) {
		if *err != nil {
			// Rollback transaction
			p.l.Debug("Rollbacking transaction")
			if e := tx.Rollback(); e != nil {
				p.l.Errorf("%s while rolling back transaction", e)
			}
		} else {
			p.l.Debug("Committing transaction")
			if e := tx.Commit(); e != nil {
				p.l.Errorf("%s while committing transaction", e)
			}
		}
	}(&err)

	// Loop through patches
	for _, patch := range patches {
		// Loop through queries
		for _, query := range p.patches[patch].queries {
			// Exec
			p.l.Debugf("Running query %s of patch %s", string(query), patch)
			if _, err = tx.Exec(string(query)); err != nil {
				p.l.Errorf("%s while executing %s", err, string(query))
				return
			}
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

	// No patches to rollback
	if len(patches) == 0 {
		p.l.Debug("No patches to rollback")
		return
	}

	// Rollback
	if err = p.rollback(patches); err != nil {
		return
	}

	// Delete last batch
	p.l.Debug("Deleting last batch")
	if err = p.storer.DeleteLastBatch(); err != nil {
		return
	}
	return
}

func (p *patcherSQL) rollback(patches []string) (err error) {
	// Start transaction
	var tx *sqlx.Tx
	if tx, err = p.conn.Beginx(); err != nil {
		return
	}
	p.l.Debug("Beginning transaction")

	// Commit/Rollback
	defer func(err *error) {
		if *err != nil {
			p.l.Debug("Rollbacking transaction")
			if e := tx.Rollback(); e != nil {
				p.l.Errorf("%s while rolling back transaction", e)
			}
		} else {
			p.l.Debug("Committing transaction")
			if e := tx.Commit(); e != nil {
				p.l.Errorf("%s while committing transaction", e)
			}
		}
	}(&err)

	// Loop through patches in reverse order
	for idx := len(patches) - 1; idx >= 0; idx-- {
		// Loop through rollbacks
		for _, rollback := range p.patches[patches[idx]].rollbacks {
			p.l.Debugf("Running rollback %s", rollback)
			if _, err = tx.Exec(string(rollback)); err != nil {
				p.l.Errorf("%s while executing %s", err, rollback)
				return
			}
		}
	}
	return
}
