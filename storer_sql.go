package astipatch

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// storerSQL represents a SQL storer
type storerSQL struct {
	conn *sqlx.DB
}

// storedPatchSQL represents SQL stored patch
type storedPatchSQL struct {
	Batch int    `db:"batch"`
	Patch string `db:"patch"`
}

// NewStorerSQL creates a new SQL storer
func NewStorerSQL(conn *sqlx.DB) Storer {
	return &storerSQL{
		conn: conn,
	}
}

// DeleteLastBatch implements the Storer interface
func (s *storerSQL) DeleteLastBatch() (err error) {
	_, err = s.conn.Exec("DELETE FROM astipatch WHERE batch = (SELECT * FROM (SELECT MAX(batch) FROM astipatch) as t)")
	return
}

// Delta implements the Storer interface
func (s *storerSQL) Delta(is []string) (os []string, err error) {
	// Fetch patches
	var ps []storedPatchSQL
	if err = s.conn.Select(&ps, "SELECT * FROM astipatch"); err != nil {
		return
	}

	// Index patches
	var ips = make(map[string]bool)
	for _, p := range ps {
		ips[p.Patch] = true
	}

	// Loop through input patches
	for _, i := range is {
		if _, ok := ips[i]; !ok {
			os = append(os, i)
		}
	}
	return
}

// Init implements the Storer interface
func (s *storerSQL) Init() (err error) {
	_, err = s.conn.Exec("CREATE TABLE IF NOT EXISTS astipatch (patch VARCHAR(255) NOT NULL, batch INT(11) NOT NULL)")
	return
}

// InsertBatch implements the Storer interface
func (s *storerSQL) InsertBatch(names []string) (err error) {
	// Fetch max batch
	var sp, max = storedPatchSQL{}, 0
	if err = s.conn.Get(&sp, "SELECT IFNULL(MAX(batch), 0) as batch FROM astipatch"); err != nil && err != sql.ErrNoRows {
		return
	} else if err == nil {
		max = sp.Batch
	}
	max++

	// Prepare values
	var values []string
	for _, name := range names {
		values = append(values, fmt.Sprintf("(\"%s\", %d)", name, max))
	}

	// Insert
	if _, err = s.conn.Exec("INSERT INTO astipatch (patch, batch) VALUES " + strings.Join(values, ",")); err != nil {
		return
	}
	return
}

// LastBatch implements the Storer interface
func (s *storerSQL) LastBatch() (ps []string, err error) {
	// Fetch last batch
	var sps []storedPatchSQL
	if err = s.conn.Select(&sps, "SELECT * FROM astipatch WHERE batch = (SELECT MAX(batch) FROM astipatch)"); err != nil && err != sql.ErrNoRows {
		return
	}

	// Loop through stored patches
	for _, p := range sps {
		ps = append(ps, p.Patch)
	}
	return
}
