# testdb

[![Go Reference](https://pkg.go.dev/badge/github.com/vaeryn-uk/go-testdb.svg)](https://pkg.go.dev/github.com/vaeryn-uk/go-testdb)

Facilitates testing for applications that use a database in Golang. This module takes the approach
of using a real database, rather than mocking them out.

Generally this is geared-towards relational/SQL databases which use migrations to define their schema.
Only Postgres handling is provided out of the box for now, but `Initializer` can be implemented to provide
initialisation logic for other vendors.

A test using this module may look something like this:

```go
package user_tests

import (
	"github.com/vaeryn-uk/go-testdb"
	"testing"
)

func TestUserLoggedIn(t *testing.T) {
	// Initialise a database. If you're using this in many tests, extract this to a helper function.
	// Note this database is removed at the end of the test automatically.
	testDb := testdb.NewPg(
		t,
		"postgresql://db/test_db?user=testuser&password=testpassword",
		testdb.CliMigrator(t, "/path/to/migrations"),
	)

	// Insert some data in to the database.
	testDb.Insert(t, "users", map[string]any{"id": 12345, "username": "Scotty"})

	// Your test code goes here, e.g. logging the user in.

	// Afterwards, you may inspect the state of the database.
	var actual bool
	testDb.QueryValue(t, "SELECT has_logged_in FROM users WHERE id = %s", &actual, 12345)

	if !actual {
		t.Fatalf("expected user 12345 to have been logged in, but they were not")
	}
}
```

Given the potential time costs of migrating a database per test, this module applies migrations
to templates and then creates databases from those templates. Support is included to intelligently
detect for changes to migrations. In practice, this means that we rarely need to do the application
migrations, cutting down on test time.

This module was born from personal projects. Feel free to use this, but consider it experimental.
Docker is a useful tool for running databases in both dev & CI environments to not have to worry about 
setup of a database server across multiple environments.