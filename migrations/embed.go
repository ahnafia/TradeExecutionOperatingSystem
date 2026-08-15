// Package migrations embeds the schema so the binary can create it without a separate
// migration tool.
//
// Phase 1 had one migration and applied it unconditionally. Phase 2 added a second, at
// which point re-running the first is an error rather than a no-op, so this now tracks
// what has been applied. Each migration runs in its own transaction alongside the row
// recording it, so a failure leaves neither the schema change nor the claim that it
// happened.
package migrations

import (
	"embed"
	"fmt"
	"sort"
	"strings"
)

//go:embed *.sql
var files embed.FS

// Migration is one schema change, named by its filename.
type Migration struct {
	Name string
	SQL  string
}

// TrackingTable creates the ledger of applied migrations. Safe to run repeatedly.
const TrackingTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    name       text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
)`

// All returns every migration in filename order. Ordering is lexical, which is why the
// files are numbered rather than named.
func All() ([]Migration, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	out := make([]Migration, 0, len(names))
	for _, n := range names {
		b, err := files.ReadFile(n)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", n, err)
		}
		out = append(out, Migration{Name: n, SQL: string(b)})
	}
	return out, nil
}
