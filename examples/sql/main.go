package main

import (
	"flag"

	"github.com/asticode/go-astilog"
	astimysql "github.com/asticode/go-astimysql"
	"github.com/asticode/go-astipatch"
	"github.com/asticode/go-astitools/flag"
	"github.com/jmoiron/sqlx"
)

func main() {
	// Subcommand
	var s = astiflag.Subcommand()
	flag.Parse()

	// Init logger
	astilog.SetLogger(astilog.New(astilog.FlagConfig()))

	// Init db
	var db *sqlx.DB
	var err error
	if db, err = astimysql.New(astimysql.FlagConfig()); err != nil {
		astilog.Fatal(err)
	}
	defer db.Close()

	// Init storer
	var st = astipatch.NewStorerSQL(db)

	// Init patcher
	var p = astipatch.NewPatcherSQL(db, st)

	// Load patches
	if err = p.Load(astipatch.FlagConfig()); err != nil {
		astilog.Fatal(err)
	}

	// Switch on subcommand
	switch s {
	case "init":
		if err = p.Init(); err != nil {
			astilog.Fatal(err)
		}
		astilog.Info("Init successful")
	case "patch":
		if err = p.Patch(); err != nil {
			astilog.Fatal(err)
		}
		astilog.Info("Patch successful")
	case "rollback":
		if err = p.Rollback(); err != nil {
			astilog.Fatal(err)
		}
		astilog.Info("Rollback successful")
	}
}
