# About

TODO

# Examples
## SQL

Run the following commands :

    $ go run examples/sql/main.go init -db-addr <the db addr> -db-name <the db name> -db-password <your value> -db-username <your value> -v
    $ go run examples/sql/main.go patch -db-addr <the db addr> -db-name <the db name> -db-password <your value> -db-username <your value> -v -astipatch-patches-directory-path examples/sql/patches/step1
    $ go run examples/sql/main.go patch -db-addr <the db addr> -db-name <the db name> -db-password <your value> -db-username <your value> -v -astipatch-patches-directory-path examples/sql/patches/step2
    $ go run examples/sql/main.go rollback -db-addr <the db addr> -db-name <the db name> -db-password <your value> -db-username <your value> -v -astipatch-patches-directory-path examples/sql/patches/step2
    $ go run examples/sql/main.go rollback -db-addr <the db addr> -db-name <the db name> -db-password <your value> -db-username <your value> -v -astipatch-patches-directory-path examples/sql/patches/step2