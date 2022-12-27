package testdb

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
)

// CliMigrator implements a migration strategy that uses the command line (linux only).
// Assumes migrate is installed: https://github.com/golang-migrate/migrate#cli-usage
func CliMigrator(t testing.TB, dir string) Migrator {
	_, err := os.ReadDir(dir)
	must(t, err)

	return &cliMigrator{dir: dir}
}

// cliMigrator is created by CliMigrator.
type cliMigrator struct {
	dir string
}

func (p *cliMigrator) Hash(t testing.TB) string {
	glob := fmt.Sprintf(path.Join(p.dir, "/*"))
	cmd := exec.Command("bash", "-c", fmt.Sprintf("md5sum %s | md5sum | awk '{ print $1 }'", glob))
	b, err := cmd.Output()

	must(t, err)

	return strings.TrimSpace(string(b))
}

func (p *cliMigrator) Migrate(t testing.TB, dsn string) {
	cmd := exec.Command(
		"migrate",
		"-database",
		dsn,
		"-path",
		p.dir, // Hardcoded migration path.
		"up",
	)
	_, err := cmd.Output()
	var exitErr *exec.ExitError

	if errors.As(err, &exitErr) {
		must(t, fmt.Errorf("failed to migrate test DB (exit code: %d): %s", exitErr.ExitCode(), exitErr.Stderr))
	}
}
