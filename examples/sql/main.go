package main

import (
	"flag"

	"github.com/asticode/go-astilog"
	"github.com/asticode/go-astipatch"
	"github.com/asticode/go-astitools/flag"
	"github.com/jmoiron/sqlx"
)

func main() {
	// Subcommand
	var s = astiflag.Subcommand()
	flag.Parse()

	// Init logger
	var l = astilog.New(astilog.FlagConfig())

	// Init db
	var db *sqlx.DB
	var err error
	if db, err = astimysql.New(astimysql.FlagConfig()); err != nil {
		l.Fatal(err)
	}
	defer db.Close()

	// Init storer
	var st = astipatch.NewStorerSQL(db)
	st.SetLogger(l)

	// Init patcher
	var p = astipatch.NewPatcherSQL(db, st)
	p.SetLogger(l)

	// Load patches
	if err = p.Load(astipatch.FlagConfig()); err != nil {
		l.Fatal(err)
	}

	// Switch on subcommand
	switch s {
	case "init":
		if err = p.Init(); err != nil {
			l.Fatal(err)
		}
		l.Info("Init successful")
	case "patch":
		if err = p.Patch(); err != nil {
			l.Fatal(err)
		}
		l.Info("Patch successful")
	case "rollback":
		if err = p.Rollback(); err != nil {
			l.Fatal(err)
		}
		l.Info("Rollback successful")
	}
}
