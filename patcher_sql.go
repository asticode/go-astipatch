package astipatch

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
)

// Vars
var (
	sqlQuerySeparator = []byte(";")
)

// patcherSQL represents a SQL patcher
type patcherSQL struct {
	*patcherBase
	conn         *sqlx.DB
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
func NewPatcherSQL(conn *sqlx.DB, s Storer) Patcher {
	return &patcherSQL{
		conn:         conn,
		patcherBase:  newPatcherBase(),
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
	p.logger.Debug("Loading patches")
	if c.PatchesDirectoryPath != "" {
		p.logger.Debugf("Patches directory is %s", c.PatchesDirectoryPath)
		if err = filepath.Walk(c.PatchesDirectoryPath, func(path string, info os.FileInfo, _ error) (err error) {
			// Log
			p.logger.Debugf("Processing %s", path)

			// Skip directories
			if info.IsDir() {
				return
			}

			// Skip none .sql files
			if filepath.Ext(path) != ".sql" {
				p.logger.Debugf("Skipping non .sql file %s", path)
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
				p.logger.Debug("No queries to add")
				return
			}

			// Add/update patch
			if _, ok := p.patches[name]; !ok {
				p.patches[name] = &patchSQL{}
				p.patchesNames = append(p.patchesNames, name)
			}
			if rollback {
				p.patches[name].rollbacks = append(p.patches[name].rollbacks, queries...)
				p.logger.Debugf("Adding %d rollback(s) to patch %s", len(queries), name)
			} else {
				p.patches[name].queries = append(p.patches[name].queries, queries...)
				p.logger.Debugf("Adding %d querie(s) to patch %s", len(queries), name)
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
	var patchesToRun []string
	if patchesToRun, err = p.storer.Delta(p.patchesNames); err != nil {
		return
	}

	// No patches to run
	if len(patchesToRun) == 0 {
		p.logger.Debug("No patches to run")
		return
	}

	// Start transaction
	var tx *sqlx.Tx
	if tx, err = p.conn.Beginx(); err != nil {
		return
	}
	p.logger.Debug("Beginning transaction")

	// Commit/Rollback
	defer func(err *error) {
		if *err != nil {
			// TODO Run rollback queries as well since mysql can't rollback on create table and alter table
			p.logger.Debug("Rollbacking")
			if e := tx.Rollback(); e != nil {
				p.logger.Errorf("%s while rolling back", e)
			}
		} else {
			p.logger.Debug("Committing")
			if e := tx.Commit(); e != nil {
				p.logger.Errorf("%s while committing", e)
			}
		}
	}(&err)

	// Loop through patches to run
	for _, patch := range patchesToRun {
		// Loop through queries
		for _, query := range p.patches[patch].queries {
			p.logger.Debugf("Running query %s of patch %s", string(query), patch)
			if _, err = tx.Exec(string(query)); err != nil {
				p.logger.Errorf("%s while executing %s", err, string(query))
				return
			}
		}
	}

	// Insert batch
	p.logger.Debug("Inserting batch")
	if err = p.storer.InsertBatch(patchesToRun); err != nil {
		return
	}
	return
}

// Rollback implements the Patcher interface
func (p *patcherSQL) Rollback() (err error) {
	// Get patches to rollback
	var patchesToRollback []string
	if patchesToRollback, err = p.storer.LastBatch(); err != nil {
		return
	}

	// No patches to rollback
	if len(patchesToRollback) == 0 {
		p.logger.Debug("No patches to rollback")
		return
	}

	// Start transaction
	var tx *sqlx.Tx
	if tx, err = p.conn.Beginx(); err != nil {
		return
	}
	p.logger.Debug("Beginning transaction")

	// Commit/Rollback
	defer func(err *error) {
		if *err != nil {
			p.logger.Debug("Rollbacking")
			if e := tx.Rollback(); e != nil {
				p.logger.Errorf("%s while rolling back", e)
			}
		} else {
			p.logger.Debug("Committing")
			if e := tx.Commit(); e != nil {
				p.logger.Errorf("%s while committing", e)
			}
		}
	}(&err)

	// Loop through patches to rollback in reverse order
	for i := len(patchesToRollback) - 1; i >= 0; i-- {
		// Loop through queries
		for _, query := range p.patches[patchesToRollback[i]].rollbacks {
			p.logger.Debugf("Running rollback %s of patch %s", string(query), patchesToRollback[i])
			if _, err = tx.Exec(string(query)); err != nil {
				p.logger.Errorf("%s while executing %s", err, string(query))
				return
			}
		}
	}

	// Delete last batch
	p.logger.Debug("Deleting last batch")
	if err = p.storer.DeleteLastBatch(); err != nil {
		return
	}
	return
}
