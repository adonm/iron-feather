// Package dbutil holds the small database/sql helpers shared by store
// (serve reads) and materialize (build/index writes): everything runs
// through caller-owned *sql.Conn handles so session state (USE, SETs)
// persists on dedicated connections.
package dbutil

import (
	"context"
	"database/sql"
	"fmt"
)

// ExecAll runs each statement in order, wrapping errors with the query.
func ExecAll(ctx context.Context, c *sql.Conn, queries ...string) error {
	for _, q := range queries {
		if _, err := c.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	return nil
}

// QueryInt scans a single int64.
func QueryInt(ctx context.Context, c *sql.Conn, q string) (int64, error) {
	var n int64
	if err := c.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// QueryStrings scans a single VARCHAR column.
func QueryStrings(ctx context.Context, c *sql.Conn, q string) ([]string, error) {
	rows, err := c.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// QueryTable scans all columns as text (*string, nil for NULL). The Go
// binding maps JSON to map[string]any and VARCHAR to string; both are
// normalized here so callers compare schema text directly.
func QueryTable(ctx context.Context, c *sql.Conn, q string) ([][]*string, error) {
	rows, err := c.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out [][]*string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]*string, len(cols))
		for i, v := range vals {
			if v == nil {
				continue
			}
			var s string
			if b, ok := v.([]byte); ok {
				s = string(b)
			} else {
				s = fmt.Sprintf("%v", v)
			}
			row[i] = &s
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
