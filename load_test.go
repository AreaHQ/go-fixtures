package fixtures

import (
	"database/sql"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

var testDbPath = "/tmp/fixtures_testdb.sqlite"

var testSchema = `CREATE TABLE some_table(
  id INT PRIMARY KEY NOT NULL,
  string_field CHAR(50) NOT NULL,
  boolean_field BOOL NOT NULL,
  created_at DATETIME,
  updated_at DATETIME
);

CREATE TABLE other_table(
  id INT PRIMARY KEY NOT NULL,
  int_field INT NOT NULL,
  boolean_field BOOL NOT NULL,
  created_at DATETIME,
  updated_at DATETIME
);

CREATE TABLE join_table(
  some_id INT NOT NULL,
  other_id INT NOT NULL,
  PRIMARY KEY(some_id, other_id)
);

CREATE TABLE string_key_table(
 id varchar(50) PRIMARY KEY NOT NULL,
 created_at DATETIME,
 updated_at DATETIME
 )`

var fixtureFile = "fixtures/test_fixtures1.yml"

var fixtureFiles = []string{
	"fixtures/test_fixtures1.yml",
	"fixtures/test_fixtures2.yml",
}

var testData = `
---

- table: 'some_table'
  pk:
    id: 1
  fields:
    string_field: 'foobar'
    boolean_field: true
    created_at: 'ON_INSERT_NOW()'
    updated_at: 'ON_UPDATE_NOW()'

- table: 'other_table'
  pk:
    id: 2
  fields:
    int_field: 123
    boolean_field: false
    created_at: 'ON_INSERT_NOW()'
    updated_at: 'ON_UPDATE_NOW()'

- table: 'join_table'
  pk:
    some_id: 1
    other_id: 2

- table: 'string_key_table'
  pk:
    id: 'new_id'
  fields:
    created_at: 'ON_INSERT_NOW()'
    updated_at: 'ON_UPDATE_NOW()'
`

func TestLoadWorksWithValidData(t *testing.T) {
	// Delete the test database
	os.Remove(testDbPath)

	var (
		db  *sql.DB
		err error
	)

	// Connect to an in-memory SQLite database
	db, err = sql.Open("sqlite3", testDbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a test schema
	_, err = db.Exec(testSchema)
	if err != nil {
		log.Fatal(err)
	}

	// Let's load the fixture, since the database is empty, this should run inserts
	err = Load([]byte(testData), db, "sqlite")

	// Error should be nil
	assert.Nil(t, err)

	var (
		count        int
		rows         *sql.Rows
		id           int
		stringField  string
		booleanField bool
		intField     int
		createdAt    *time.Time
		updatedAt    *time.Time
		someID       int
		otherID      int
	)

	// Check row counts
	db.QueryRow("SELECT COUNT(*) FROM some_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM other_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM join_table").Scan(&count)
	assert.Equal(t, 1, count)

	db.QueryRow("SELECT COUNT(*) FROM string_key_table").Scan(&count)
	assert.Equal(t, 1, count)

	// Check correct data has been loaded into some_table
	rows, err = db.Query("SELECT id, string_field, boolean_field, " +
		"created_at, updated_at FROM some_table")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&id,
			&stringField,
			&booleanField,
			&createdAt,
			&updatedAt,
		); err != nil {
			log.Fatal(err)
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, 1, id)
		assert.Equal(t, "foobar", stringField)
		assert.Equal(t, true, booleanField)
		assert.NotNil(t, createdAt)
		assert.Nil(t, updatedAt)
	}

	// Check correct data has been loaded into other_table
	rows, err = db.Query("SELECT id, int_field, boolean_field, " +
		"created_at, updated_at FROM other_table")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&id,
			&intField,
			&booleanField,
			&createdAt,
			&updatedAt,
		); err != nil {
			log.Fatal(err)
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, 2, id)
		assert.Equal(t, 123, intField)
		assert.Equal(t, false, booleanField)
		assert.NotNil(t, createdAt)
		assert.Nil(t, updatedAt)
	}

	// Check correct data has been loaded into join_table
	rows, err = db.Query("SELECT some_id, other_id FROM join_table")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&someID,
			&otherID,
		); err != nil {
			log.Fatal(err)
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, 1, someID)
		assert.Equal(t, 2, otherID)
	}

	// Let's reload the fixture, this should run updates
	err = Load([]byte(testData), db, "sqlite")

	// Error should be nil
	assert.Nil(t, err)

	// Check row counts, should be unchanged
	db.QueryRow("SELECT COUNT(*) FROM some_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM other_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM join_table").Scan(&count)
	assert.Equal(t, 1, count)

	// Check correct data has been loaded into some_table
	rows, err = db.Query("SELECT id, string_field, boolean_field, " +
		"created_at, updated_at FROM some_table")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&id,
			&stringField,
			&booleanField,
			&createdAt,
			&updatedAt,
		); err != nil {
			log.Fatal(err)
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, 1, id)
		assert.Equal(t, "foobar", stringField)
		assert.Equal(t, true, booleanField)
		assert.NotNil(t, createdAt)
		assert.NotNil(t, updatedAt)
	}

	// Check correct data has been loaded into other_table
	rows, err = db.Query("SELECT id, int_field, boolean_field, " +
		"created_at, updated_at FROM other_table")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&id,
			&intField,
			&booleanField,
			&createdAt,
			&updatedAt,
		); err != nil {
			log.Fatal(err)
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, 2, id)
		assert.Equal(t, 123, intField)
		assert.Equal(t, false, booleanField)
		assert.NotNil(t, createdAt)
		assert.NotNil(t, updatedAt)
	}

	// Check correct data has been loaded into join_table
	rows, err = db.Query("SELECT some_id, other_id FROM join_table")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&someID,
			&otherID,
		); err != nil {
			log.Fatal(err)
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, 1, someID)
		assert.Equal(t, 2, otherID)
	}
}

func TestLoadFileWorksWithValidFile(t *testing.T) {
	// Delete the test database
	os.Remove(testDbPath)

	var (
		db  *sql.DB
		err error
	)

	// Connect to an in-memory SQLite database
	db, err = sql.Open("sqlite3", testDbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a test schema
	_, err = db.Exec(testSchema)
	if err != nil {
		log.Fatal(err)
	}

	var count int
	// Check row counts to show no data
	db.QueryRow("SELECT COUNT(*) FROM some_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM other_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM join_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM string_key_table").Scan(&count)
	assert.Equal(t, 0, count)

	// Let's load the fixture, since the database is empty, this should run inserts
	err = LoadFile(fixtureFile, db, "sqlite")

	// Error should be nil
	assert.Nil(t, err)

	var (
		rows         *sql.Rows
		id           int
		stringField  string
		booleanField bool
		createdAt    *time.Time
		updatedAt    *time.Time
	)

	// Check row counts
	db.QueryRow("SELECT COUNT(*) FROM some_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM other_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM join_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM string_key_table").Scan(&count)
	assert.Equal(t, 0, count)

	// Check correct data has been loaded into some_table
	rows, err = db.Query("SELECT id, string_field, boolean_field, " +
		"created_at, updated_at FROM some_table")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&id,
			&stringField,
			&booleanField,
			&createdAt,
			&updatedAt,
		); err != nil {
			log.Fatal(err)
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, 1, id)
		assert.Equal(t, "foobar", stringField)
		assert.Equal(t, true, booleanField)
		assert.NotNil(t, createdAt)
		assert.Nil(t, updatedAt)
	}

	// Let's reload the fixture, this should run updates
	err = LoadFile(fixtureFile, db, "sqlite")

	// Error should be nil
	assert.Nil(t, err)

	// Check row counts, should be unchanged
	db.QueryRow("SELECT COUNT(*) FROM some_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM other_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM join_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM string_key_table").Scan(&count)
	assert.Equal(t, 0, count)
}

func TestLoadFileFailssWithMissingFile(t *testing.T) {
	// Delete the test database
	os.Remove(testDbPath)

	var (
		db  *sql.DB
		err error
	)

	// Connect to an in-memory SQLite database
	db, err = sql.Open("sqlite3", testDbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a test schema
	_, err = db.Exec(testSchema)
	if err != nil {
		log.Fatal(err)
	}

	// Let's load the fixture, since the database is empty, this should run inserts
	err = LoadFile("bad_filename.yml", db, "sqlite")

	// Error should be nil
	assert.EqualError(t, err, "open bad_filename.yml: no such file or directory", "Expected file not found error")
}

func TestLoadFilesWorksWithValidFiles(t *testing.T) {
	// Delete the test database
	os.Remove(testDbPath)

	var (
		db  *sql.DB
		err error
	)

	// Connect to an in-memory SQLite database
	db, err = sql.Open("sqlite3", testDbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a test schema
	_, err = db.Exec(testSchema)
	if err != nil {
		log.Fatal(err)
	}

	var count int

	// Check rows are empty first
	db.QueryRow("SELECT COUNT(*) FROM some_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM other_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM join_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM string_key_table").Scan(&count)
	assert.Equal(t, 0, count)

	// Let's load the fixture, since the database is empty, this should run inserts
	err = LoadFiles(fixtureFiles, db, "sqlite")

	// Error should be nil
	assert.Nil(t, err)

	// Check row counts
	db.QueryRow("SELECT COUNT(*) FROM some_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM other_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM join_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM string_key_table").Scan(&count)
	assert.Equal(t, 1, count)

	// Let's reload the fixtures, this should run updates
	err = LoadFiles(fixtureFiles, db, "sqlite")

	// Error should be nil
	assert.Nil(t, err)

	// Check row counts, should be unchanged
	db.QueryRow("SELECT COUNT(*) FROM some_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM other_table").Scan(&count)
	assert.Equal(t, 1, count)
	db.QueryRow("SELECT COUNT(*) FROM join_table").Scan(&count)
	assert.Equal(t, 1, count)
}

func TestLoadFilesFailsWithABadFile(t *testing.T) {
	// Delete the test database
	os.Remove(testDbPath)

	var (
		db  *sql.DB
		err error
	)

	// Connect to an in-memory SQLite database
	db, err = sql.Open("sqlite3", testDbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a test schema
	_, err = db.Exec(testSchema)
	if err != nil {
		log.Fatal(err)
	}

	var count int

	// Check rows are empty first
	db.QueryRow("SELECT COUNT(*) FROM some_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM other_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM join_table").Scan(&count)
	assert.Equal(t, 0, count)
	db.QueryRow("SELECT COUNT(*) FROM string_key_table").Scan(&count)
	assert.Equal(t, 0, count)

	var badList = []string{
		fixtureFile,
		"bad_file",
	}

	// Let's load the fixture, since the database is empty, this should run inserts
	err = LoadFiles(badList, db, "sqlite")

	// Error should be nil
	assert.EqualError(t, err, "open bad_file: no such file or directory", "Expected file not found error")

}

func TestCheckPostgresPKWorks(t *testing.T) {
	expected := "SELECT data_type " +
		"FROM information_schema.columns " +
		"WHERE table_name='test_table' " +
		"AND column_name='id';"

	actual := checkPostgresPKDataType("test_table")

	assert.Equal(t, actual, expected, "Data type sql should match")
}

func TestFixPostgresSequenceWorks(t *testing.T) {
	expected := "SELECT pg_catalog.setval(" +
		"pg_get_serial_sequence('test_table', 'id'), " +
		"(SELECT MAX(id) FROM test_table));"

	actual := fixPostgresPKSequence("test_table")

	assert.Equal(t, actual, expected, "Sequence fix sql should match")
}
