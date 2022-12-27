package testdb

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"hash/crc32"
	"regexp"
	"strings"
	"testing"
)

// NewPg initialises a new Postgres test database at the database indicated by dsn.
// dsn must be a valid connection that has permission to create new databases.
// Returns the Db handle representing a fully migrated, isolated database ready
// for use in your test.
func NewPg(t testing.TB, dsn string, migrator Migrator) Db {
	return New[*pgx.Conn](t, dsn, &pgInitializer{}, migrator)
}

type PgDb struct {
	name    string
	dsn     string
	rootDsn string
}

func (p *PgDb) Name() string {
	return p.dsn
}

func (p *PgDb) Dsn() string {
	return p.dsn
}

func (p *PgDb) Insert(t testing.TB, table string, data ...map[string]any) {
	t.Helper()

	conn := p.connect(t, p.dsn)
	defer conn.Close(context.Background())

	for _, entry := range data {
		args := make([]any, 0, len(entry))
		cols := make([]string, 0, len(entry))
		vals := make([]string, 0, len(entry))
		i := 1
		for name, val := range entry {
			args = append(args, val)
			cols = append(cols, name)
			vals = append(vals, fmt.Sprintf("$%d", i))
			i++
		}

		query := fmt.Sprintf(
			"INSERT INTO %s(%s) VALUES(%s)",
			table,
			strings.Join(cols, ","),
			strings.Join(vals, ","),
		)

		_, err := conn.Exec(context.Background(), query, args...)
		must(t, err)
	}
}

func (p *PgDb) QueryValue(t testing.TB, sql string, into any, args ...any) {
	// TODO implement me
	panic("implement me")
}

func (p *PgDb) Drop(t testing.TB) {
	root := p.connect(t, p.rootDsn)

	// Forcibly close any remaining connections
	closeConns := `
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = '%s'`

	_, err := root.Exec(context.Background(), fmt.Sprintf(closeConns, verifyPgDbName(t, p.name)))
	must(t, err)

	_, err = root.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS \"%s\"", verifyPgDbName(t, p.name)))
	must(t, err)
}

func (p *PgDb) connect(t testing.TB, dsn string) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), dsn)
	must(t, err)
	return conn
}

type pgInitializer struct{}

func (p *pgInitializer) Connect(t testing.TB, dsn string) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), dsn)
	must(t, err)
	return conn
}

func (p *pgInitializer) Lock(t testing.TB, conn *pgx.Conn, name string) {
	_, err := conn.Exec(context.Background(), "SELECT pg_advisory_lock($1)", crc32.ChecksumIEEE([]byte(name)))
	must(t, err)
}

func (p *pgInitializer) Unlock(t testing.TB, conn *pgx.Conn, name string) {
	_, err := conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", crc32.ChecksumIEEE([]byte(name)))
	must(t, err)
}

func (p *pgInitializer) Exists(t testing.TB, conn *pgx.Conn, name string) bool {
	row := conn.QueryRow(context.Background(), "SELECT true FROM pg_database WHERE datname = $1", name)

	var exists bool
	err := row.Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}

	must(t, err)

	return exists
}

func (p *pgInitializer) Create(t testing.TB, conn *pgx.Conn, name string) {
	_, err := conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE \"%s\"", verifyPgDbName(t, name)))
	must(t, err)
}

func (p *pgInitializer) CreateFromTemplate(t testing.TB, conn *pgx.Conn, template, name string) {
	_, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE \"%s\" TEMPLATE \"%s\"",
		verifyPgDbName(t, name),
		verifyPgDbName(t, template),
	))
	must(t, err)
}

func (p *pgInitializer) NewDsn(t testing.TB, base string, newName string) string {
	// Ideally would use pgx.ConnConfig parsing, but doesn't seem to allow
	// us to tweak it and then return a new ConnString(). So hacking with
	// regex for now.
	r := regexp.MustCompile("/\\w+\\?")

	if !r.MatchString(base) {
		ErrorHandler(t, fmt.Errorf("invalid DSN provided, cannot find database name in `%s`", base))
	}

	return r.ReplaceAllString(base, fmt.Sprintf("/%s?", newName))
}

func (p *pgInitializer) Remove(t testing.TB, conn *pgx.Conn, name string) {
	_, err := conn.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", verifyPgDbName(t, name)))
	must(t, err)
}

func (p *pgInitializer) NewDb(t testing.TB, rootDsn, dsn string) Db {
	conf, err := pgx.ParseConfig(dsn)
	must(t, err)

	return &PgDb{
		name:    conf.Database,
		dsn:     dsn,
		rootDsn: rootDsn,
	}
}

var pgDbNameRegex = regexp.MustCompile("^[a-zA-z0-9_]+$")

func verifyPgDbName(t testing.TB, name string) string {
	if !pgDbNameRegex.MatchString(name) {
		ErrorHandler(t, fmt.Errorf("%s as a DB name may be unsafe. letters, numbers and _ only", name))
	}

	return name
}
