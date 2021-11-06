Patch manager written in GO.

# Languages
## SQL

Patches are described as an array of transactions in `yaml`. Each transaction can contain multiple queries and/or rollbacks.

A transaction **must** only describe:
- either one [statement causing an implicit commit](https://dev.mysql.com/doc/refman/8.0/en/implicit-commit.html)
- or multiple statements not causing an implicit commit

```yml
-
  queries:
    - >      
      CREATE TABLE test (
        name VARCHAR(255) NOT NULL
      )
  rollbacks:
    - DROP TABLE test
-
  queries:
    - INSERT INTO test (name) VALUES ('test 1'), ('test2')
    - UPDATE test SET name = 'test 2' WHERE id = 2
  rollbacks:
    - DELETE FROM test WHERE id IN (1, 2)
-
  queries:
    - ALTER TABLE test ADD id int NOT NULL AUTO_INCREMENT PRIMARY KEY
  rollbacks:
    - ALTER TABLE test DROP COLUMN id
```