package testdb

import (
	"fmt"
	"github.com/google/uuid"
	"strings"
	"testing"
)

// Db is the core handle provided to your tests. This represents a fully migrated,
// test database ready for use. This database is always brand new, so has isolation from
// other tests. Use New or its variants to get one of these.
type Db interface {
	// The Name of the test database.
	Name() string
	// Dsn is the data source name of the test database; a connection string.
	Dsn() string
	// Insert will insert the provided data into table within the test database.
	// Each data is expected to be a col => val mapping.
	// For convenience, multiple data entries may be provided; each will be inserted
	// separate as a new row.
	Insert(t testing.TB, table string, data ...map[string]any)
	// QueryValue will runs sql with args, writing a single value back to into. Provide
	// a pointer for into.
	QueryValue(t testing.TB, sql string, into any, args ...any)
	// Drop forcefully removes the database. This is automatically done as part of database
	// cleanup.
	Drop(t testing.TB)
}

// Migrator is responsible for applying migrations to a database.
type Migrator interface {
	// Hash the current state of migrations. This should return a different value
	// any time the migration definitions are changed. Commonly, this involves hashing
	// the contents of a migrations directory.
	Hash(t testing.TB) string
	// Migrate applies the current migrations to the provided dsn (data source name).
	Migrate(t testing.TB, dsn string)
}

type Initializer[Conn any] interface {
	// Connect returns an active connection to the provided DSN.
	Connect(t testing.TB, dsn string) Conn

	// Lock provides some protection against concurrent migration application for parallel
	// tests. This should ensure that only one testdb initialization is happening. Should
	// lock against the provided name. This may be done with stdlib sync stuff, or better
	// yet, at the database itself if possible.
	// This will block until the lock is acquired.
	Lock(t testing.TB, conn Conn, name string)

	// Unlock releases the lock acquired in Lock.
	Unlock(t testing.TB, conn Conn, name string)

	// Exists checks if the database with name exists in the database already.
	Exists(t testing.TB, conn Conn, name string) bool

	// Create a new blank database with the given name.
	Create(t testing.TB, conn Conn, name string)

	// CreateFromTemplate creates a new database with name, using a template database
	// with name template.
	CreateFromTemplate(t testing.TB, conn Conn, template, name string)

	// NewDsn takes a base DSN and returns a new one with the given newName
	// as the database name. This will override the dbname portion of dsn
	// with the new name.
	NewDsn(t testing.TB, base string, newName string) string

	// NewDb creates a new Db. rootDsn is the connection to the database
	// used to manage database creation etc.; dsn is the connection string
	// for the newly-created test database.
	NewDb(t testing.TB, rootDsn, dsn string) Db

	// Remove removes the database given by name entirely using the provided
	// root connection.
	Remove(t testing.TB, conn Conn, name string)
}

// ErrorHandler will be invoked for any error throughout testdb initialisation
// or interaction. This is expected to halt & fail the test
// immediately. You may override this for custom outputs.
var ErrorHandler = func(t testing.TB, err error, extra ...any) {
	t.Helper()

	t.Fatal(append([]any{"testdb initialisation failed", err}, extra...))
}

// New initialises a new test database at the database indicated by dsn.
// dsn must be a valid connection that has permission to create new databases.
// Returns the Db handle representing a fully migrated, isolated database ready
// for use in your test.
//
// You may want to use a ready-provided constructor, such as NewPg. This is exposed
// for custom initializers if you're using a database that isn't supported.
func New[Conn any](t testing.TB, dsn string, h Initializer[Conn], m Migrator) Db {
	root := h.Connect(t, dsn)

	migrationHash := m.Hash(t)
	templateName := fmt.Sprintf("test_template_%s", migrationHash)

	h.Lock(t, root, templateName)

	if !h.Exists(t, root, templateName) {
		h.Create(t, root, templateName)

		done := false
		// Due to our halting error handling, here we add an explicit check
		// to see if the migration has applied. If not, remove the template
		// DB as it'll be corrupt/bad.
		t.Cleanup(func() {
			if !done {
				h.Remove(t, root, templateName)
			}
		})

		m.Migrate(t, h.NewDsn(t, dsn, templateName))
		done = true
	}

	testDbName := fmt.Sprintf("test_db_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
	h.CreateFromTemplate(t, root, templateName, testDbName)

	h.Unlock(t, root, templateName)

	testDbDsn := h.NewDsn(t, dsn, testDbName)

	db := h.NewDb(t, dsn, testDbDsn)

	// Remove the DB when we're done with it.
	t.Cleanup(func() {
		db.Drop(t)
	})

	return db
}

func must(t testing.TB, err error, extra ...any) {
	t.Helper()

	if err != nil {
		ErrorHandler(t, err, extra)
	}
}
