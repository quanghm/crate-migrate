//go:build go1.20
// +build go1.20

package crate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/atomic"

	migrate "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/multistmt"
	"github.com/hashicorp/go-multierror"
	"github.com/lib/pq"
)

func init() {
	db := Crate{}

	// Register the driver
	database.Register("crate", &db)
	database.Register("cratedb", &db)
}

var (
	multiStmtDelimiter = []byte(";")

	DefaultMigrationsTable       = "schema_migrations"
	DefaultLockTable             = "schema_migrations_lock"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10MB
)

var (
	ErrNilConfig      = errors.New("no config")
	ErrNoDatabaseName = errors.New("no database name")
	ErrNoSchema       = errors.New("no schema")
	ErrDatabaseDirty  = errors.New("database is dirty")
)

type Config struct {
	MigrationsSchemaName  string
	MigrationsTableName   string
	MigrationsTableQuoted bool
	MultiStatementEnabled bool
	LockTableName         string
	DatabaseName          string
	StatementTimeout      time.Duration
	MultiStatementMaxSize int
}

type Crate struct {
	// Database connection string
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool

	config *Config
}

func WithConnection(ctx context.Context, conn *sql.Conn, config *Config) (*Crate, error) {
	if conn == nil {
		return nil, ErrNilConfig
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		// pull the database name from the connection
		query := `SELECT CURRENT_DATABASE()`
		if err := conn.QueryRowContext(ctx, query).Scan(&config.DatabaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		// if the database name is empty still, then return an error
		if config.DatabaseName == "" {
			return nil, ErrNoDatabaseName
		}
	}

	if config.MigrationsSchemaName == "" {
		// pull the schema name from the connection
		query := `SELECT CURRENT_SCHEMA()`

		var schemaName sql.NullString
		if err := conn.QueryRowContext(ctx, query).Scan(&schemaName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		// if the schema name is empty still, then return an error
		if !schemaName.Valid {
			return nil, ErrNoSchema
		}

		config.MigrationsSchemaName = schemaName.String
	}

	if config.MigrationsTableQuoted {
		re := regexp.MustCompile(`"(.*?)"`)
		result := re.FindAllStringSubmatch(config.MigrationsTableName, -1)
		config.MigrationsTableName = result[len(result)-1][1]
		if len(result) == 2 {
			config.MigrationsSchemaName = result[0][1]
		} else if len(result) > 2 {
			return nil, fmt.Errorf("\"%s\" MigrationsTable contains too many dot characters", config.MigrationsTableName)
		}
	}

	if config.MigrationsTableName == "" {
		config.MigrationsTableName = DefaultMigrationsTable
	}

	crate := &Crate{
		conn:   conn,
		config: config,
	}

	if err := crate.ensureLockTable(); err != nil {
		return nil, err
	}

	if err := crate.ensureVersionTable(); err != nil {
		return nil, err
	}

	return crate, nil
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	ctx := context.Background()

	if err := instance.PingContext(ctx); err != nil {
		return nil, err
	}

	conn, err := instance.Conn(ctx)
	if err != nil {
		return nil, err
	}

	crate, err := WithConnection(ctx, conn, config)
	if err != nil {
		return nil, err
	}
	crate.db = instance

	return crate, nil
}

func (c *Crate) Open(connStr string) (database.Driver, error) {
	purl, err := url.Parse(connStr)
	if err != nil {
		return nil, err
	}

	re := regexp.MustCompile("^(crate|cratedb)")
	connectString := re.ReplaceAllString(migrate.FilterCustomQuery(purl).String(), "postgres")

	db, err := sql.Open("postgres", connectString)
	if err != nil {
		return nil, err
	}
	migrationsTable := purl.Query().Get("x-migrations-table")
	migrationsTableQuoted := false
	if s := purl.Query().Get("x-migrations-table-quoted"); s != "" {
		migrationsTableQuoted, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("invalid x-migrations-table-quoted: %w", err)
		}
	}

	if (migrationsTable != "") && (migrationsTableQuoted) && ((migrationsTable[0] != '"') || (migrationsTable[len(migrationsTable)-1] != '"')) {
		return nil, fmt.Errorf("x-migrations-table must be quoted (for instance '\"migrate\".\"schema_migrations\"') when x-migrations-table-quoted is enabled, current value is: %s", migrationsTable)
	}

	statementTimeoutString := purl.Query().Get("x-statement-timeout")
	statementTimeout := 0
	if statementTimeoutString != "" {
		statementTimeout, err = strconv.Atoi(statementTimeoutString)
		if err != nil {
			return nil, err
		}
	}

	lockTable := purl.Query().Get("x-lock-table")
	if lockTable == "" {
		lockTable = DefaultLockTable
	}

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := purl.Query().Get("x-multi-statement-max-size"); s != "" {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		if multiStatementMaxSize <= 0 {
			multiStatementMaxSize = DefaultMultiStatementMaxSize
		}
	}

	multiStatementEnabled := false
	if s := purl.Query().Get("x-multi-statement"); s != "" {
		multiStatementEnabled, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("unable to parse option x-multi-statement: %w", err)
		}
	}

	crate, err := WithInstance(db, &Config{
		DatabaseName:          purl.Path,
		MigrationsTableName:   migrationsTable,
		MigrationsTableQuoted: migrationsTableQuoted,
		StatementTimeout:      time.Duration(statementTimeout) * time.Millisecond,
		MultiStatementEnabled: multiStatementEnabled,
		MultiStatementMaxSize: multiStatementMaxSize,
		LockTableName:         lockTable,
	})

	if err != nil {
		return nil, err
	}

	return crate, nil
}

func (c *Crate) Close() error {
	connErr := c.conn.Close()
	var dbErr error
	if c.db != nil {
		dbErr = c.db.Close()
	}

	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

func (c *Crate) Lock() error {
	return database.CasRestoreOnErr(&c.isLocked, false, true, database.ErrLocked, func() (err error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		lockID, err := database.GenerateAdvisoryLockId(c.config.DatabaseName)
		if err != nil {
			return err
		}

		query := "SELECT COUNT(1) FROM " + pq.QuoteIdentifier(c.config.MigrationsSchemaName) + "." + pq.QuoteIdentifier(c.config.LockTableName) + " WHERE lock_id = $1"
		row := c.conn.QueryRowContext(context.Background(), query, lockID)

		var count int
		err = row.Scan(&count)
		if err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		if count > 0 {
			return database.ErrLocked
		}

		query = "INSERT INTO " + pq.QuoteIdentifier(c.config.MigrationsSchemaName) + "." + c.config.LockTableName + " (lock_id) VALUES ($1)"
		if _, err = c.conn.ExecContext(ctx, query, lockID); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		return nil
	})
}

func (c *Crate) Unlock() error {
	return database.CasRestoreOnErr(&c.isLocked, true, false, database.ErrNotLocked, func() (err error) {
		lockID, err := database.GenerateAdvisoryLockId(c.config.DatabaseName)
		if err != nil {
			return err
		}

		// In the event of an implementation (non-migration) error, it is possible for the lock to not be released.  Until
		// a better locking mechanism is added, a manual purging of the lock table may be required in such circumstances
		query := "DELETE FROM " + pq.QuoteIdentifier(c.config.MigrationsSchemaName) + "." + c.config.LockTableName + " WHERE lock_id = $1"
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		if _, err := c.conn.ExecContext(ctx, query, lockID); err != nil {
			return database.Error{OrigErr: err, Err: "failed to release migration lock", Query: []byte(query)}
		}

		return nil
	})
}

func (c *Crate) Run(migration io.Reader) error {
	if c.config.MultiStatementEnabled {
		var err error
		if e := multistmt.Parse(migration, multiStmtDelimiter, c.config.MultiStatementMaxSize, func(m []byte) bool {
			if err = c.runStatement(m); err != nil {
				return false
			}
			return true
		}); e != nil {
			return e
		}
		return err
	}
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	return c.runStatement(migr)
}

func (c *Crate) runStatement(statement []byte) error {
	ctx := context.Background()
	if c.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.StatementTimeout)
		defer cancel()
	}
	query := string(statement)
	if strings.TrimSpace(query) == "" {
		return nil
	}

	if _, err := c.conn.ExecContext(ctx, query); err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			var line uint
			var col uint
			var lineColOK bool
			if pgErr.Position != "" {
				if pos, parseErr := strconv.ParseUint(pgErr.Position, 10, 64); parseErr == nil {
					line, col, lineColOK = computeLineFromPos(query, int(pos))
				}
			}
			message := fmt.Sprintf("migration failed: %s", pgErr.Message)
			if lineColOK {
				message = fmt.Sprintf("%s (column %d)", message, col)
			}
			if pgErr.Detail != "" {
				message = fmt.Sprintf("%s, %s", message, pgErr.Detail)
			}
			return database.Error{OrigErr: err, Err: message, Query: statement, Line: line}
		}
		return database.Error{OrigErr: err, Err: "migration failed", Query: statement}
	}
	return nil
}

func computeLineFromPos(s string, pos int) (line, col uint, ok bool) {
	// replace crlf with lf
	s = strings.ReplaceAll(s, "\r\n", "\n")
	// pg docs: pos uses index 1 for the first character, and positions are measured in characters not bytes
	runes := []rune(s)
	if pos > len(runes) {
		return 0, 0, false
	}
	sel := runes[:pos]
	line = uint(runesCount(sel, newLine) + 1)
	col = uint(pos - 1 - runesLastIndex(sel, newLine))
	return line, col, true
}

const newLine = '\n'

func runesCount(input []rune, target rune) int {
	var count int
	for _, r := range input {
		if r == target {
			count++
		}
	}
	return count
}

func runesLastIndex(input []rune, target rune) int {
	for i := len(input) - 1; i >= 0; i-- {
		if input[i] == target {
			return i
		}
	}
	return -1
}

func (c *Crate) SetVersion(version int, dirty bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	query := `DELETE FROM ` + c.migrationsTableWithSchema() + ` WHERE version >= $1`
	if _, err := c.conn.ExecContext(ctx, query, version); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		// TODO: For some reason this always returns a primary-key-conflict error without the ON CONFLICT clause
		query = `INSERT INTO ` + c.migrationsTableWithSchema() + ` (version, dirty) VALUES ($1, $2)
			ON CONFLICT (version) DO UPDATE SET dirty = $2
		`
		if _, err := c.conn.ExecContext(ctx, query, version, dirty); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

func (c *Crate) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + c.migrationsTableWithSchema() + ` LIMIT 1`
	err = c.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			if e.Code.Name() == "undefined_table" {
				return database.NilVersion, false, nil
			}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (c *Crate) Drop() (err error) {
	// select all tables in current schema
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema=(SELECT current_schema()) AND table_type='BASE TABLE'`
	tables, err := c.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	// delete one table after another
	tableNames := make([]string, 0)
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return err
		}
		if tableName != "" {
			tableNames = append(tableNames, tableName)
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(tableNames) > 0 {
		// delete one by one ...
		for _, t := range tableNames {
			query = `DROP TABLE IF EXISTS ` + pq.QuoteIdentifier(t) + ` CASCADE`
			if _, err := c.conn.ExecContext(context.Background(), query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

func (c *Crate) ensureVersionTable() (err error) {
	err = c.Lock()
	if err != nil {
		return err
	}

	defer func() {
		if e := c.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	// This block checks whether the `MigrationsTable` already exists. This is useful because it allows read only postgres
	// users to also check the current version of the schema. Previously, even if `MigrationsTable` existed, the
	// `CREATE TABLE IF NOT EXISTS...` query would fail because the user does not have the CREATE permission.
	// Taken from https://github.com/mattes/migrate/blob/master/database/postgres/postgres.go#L258
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`
	row := c.conn.QueryRowContext(context.Background(), query, c.config.MigrationsSchemaName, c.config.MigrationsTableName)

	var count int
	err = row.Scan(&count)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if count == 1 {
		return nil
	}

	query = `CREATE TABLE IF NOT EXISTS ` + c.migrationsTableWithSchema() + ` (version bigint not null primary key, dirty boolean not null)`
	if _, err = c.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (c *Crate) ensureLockTable() error {
	query := `CREATE TABLE IF NOT EXISTS ` + pq.QuoteIdentifier(c.config.MigrationsSchemaName) + "." + pq.QuoteIdentifier(c.config.LockTableName) + ` (lock_id bigint not null primary key)`
	_, err := c.conn.ExecContext(context.Background(), query)
	return err
}

func (c *Crate) migrationsTableWithSchema() string {
	return pq.QuoteIdentifier(c.config.MigrationsSchemaName) + "." + pq.QuoteIdentifier(c.config.MigrationsTableName)
}
