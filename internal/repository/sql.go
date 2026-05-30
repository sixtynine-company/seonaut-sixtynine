package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/stjudewashere/seonaut/internal/config"
)

const (
	// paginationMax is the maximum number of items allowed in paginated lists
	paginationMax = 25

	// maxOpenConns is the maximum number of open connections to the database.
	// Use 0 for unlimited connections. Kept well below MySQL's max_connections so a
	// burst of crawl writes leaves headroom for interactive web reads.
	maxOpenConns = 50

	// maxIddleConns is the maximum number of connections in the idle connection pool.
	// Use 0 for no idle connections retained.
	maxIddleConns = 50

	// connMaxLifeInMinutes is the maximum amount of time a connection may be reused.
	// Use 0 to not close connections due to it's age.
	connMaxLifeInMinutes = 5

	// connMaxIdleInMinutes is how long an idle connection is kept before it is closed,
	// so connections opened for a crawl burst are released back promptly afterwards.
	connMaxIdleInMinutes = 2

	// Per-call DB operation timeouts. Every repository call derives a context with one
	// of these so that waiting for a query — including waiting for a free pooled
	// connection — can never block a request forever and wedge the whole server.
	dbReadTimeout   = 15 * time.Second  // single-shot SELECT / QueryRow
	dbWriteTimeout  = 30 * time.Second  // INSERT / UPDATE / DELETE / Prepare
	dbStreamTimeout = 120 * time.Second // queries streamed row-by-row over a channel
)

// readCtx returns a context bounded by dbReadTimeout for single-shot read queries.
func readCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), dbReadTimeout)
}

// writeCtx returns a context bounded by dbWriteTimeout for writes and prepared inserts.
func writeCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), dbWriteTimeout)
}

// streamCtx returns a context bounded by dbStreamTimeout for queries whose rows are
// streamed over a channel for the lifetime of consuming the stream (exports, the
// multipage reporters and the per-crawl page iterators).
func streamCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), dbStreamTimeout)
}

// SqlConnect creates a new SQL connection with the provided configuration.
func SqlConnect(config *config.DBConfig) (*sql.DB, error) {
	// timeout/readTimeout/writeTimeout are socket-level deadlines on the connection to
	// MySQL. They are a backstop below "forever" for a wedged/half-open connection; the
	// per-call context deadlines above are the primary bound. Kept generous so they
	// never cut a legitimately slow streaming read before its context does.
	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&timeout=10s&readTimeout=60s&writeTimeout=60s",
		config.User,
		config.Pass,
		config.Server,
		config.Port,
		config.Name,
	))

	if err != nil {
		return nil, err
	}

	// Set maximum number of open connections to the database.
	db.SetMaxOpenConns(maxOpenConns)

	// Set maximum number of idle connections to the database.
	db.SetMaxIdleConns(maxIddleConns)

	// Set maximum lifetime for each connection to the database.
	db.SetConnMaxLifetime(connMaxLifeInMinutes * time.Minute)

	// Release idle connections promptly so a crawl burst does not pin the pool.
	db.SetConnMaxIdleTime(connMaxIdleInMinutes * time.Minute)

	// Ping the database to check if the connection is successful.
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

// Hash returns a hashed string.
func Hash(s string) string {
	hash := sha256.Sum256([]byte(s))

	return hex.EncodeToString(hash[:])
}

// Truncate a string to the requiered length.
func Truncate(s string, length int) string {
	text := []rune(s)
	if len(text) > length {
		s = string(text[:length-3]) + "..."
	}

	return s
}
