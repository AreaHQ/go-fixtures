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
)`

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
`

func TestLoad(t *testing.T) {
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
