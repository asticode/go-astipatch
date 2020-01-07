package main

import (
	"flag"
	"log"

	"github.com/asticode/go-astikit"
	"github.com/asticode/go-astimysql"
	"github.com/asticode/go-astipatch"
	"github.com/jmoiron/sqlx"
)

func main() {
	// Flags
	cmd := astikit.FlagCmd()
	flag.Parse()

	// Create logger
	l := log.New(log.Writer(), log.Prefix(), log.Flags())

	// Init db
	var db *sqlx.DB
	var err error
	if db, err = astimysql.New(astimysql.FlagConfig()); err != nil {
		l.Fatal(err)
	}
	defer db.Close()

	// Init storer
	var st = astipatch.NewStorerSQL(db)

	// Init patcher
	var p = astipatch.NewPatcherSQL(db, st, l)

	// Load patches
	if err = p.Load(astipatch.FlagConfig()); err != nil {
		l.Fatal(err)
	}

	// Switch on subcommand
	switch cmd {
	case "init":
		if err = p.Init(); err != nil {
			l.Fatal(err)
		}
		l.Println("Init successful")
	case "patch":
		if err = p.Patch(); err != nil {
			l.Fatal(err)
		}
		l.Println("Patch successful")
	case "rollback":
		if err = p.Rollback(); err != nil {
			l.Fatal(err)
		}
		l.Println("Rollback successful")
	}
}
