package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
)

// EngineType defines the underlying database dialect.
type EngineType string

const (
	EnginePostgres EngineType = "postgres"
	EngineMySQL    EngineType = "mysql"
)

// DetectEngine automatically detects the database engine type from the connection string or URI.
func DetectEngine(connStr string) (EngineType, error) {
	s := strings.TrimSpace(connStr)
	if strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://") {
		return EnginePostgres, nil
	}
	if strings.HasPrefix(s, "mysql://") || strings.HasPrefix(s, "mariadb://") {
		return EngineMySQL, nil
	}
	// Also detect standard MySQL DSN patterns (e.g. user:pass@tcp(host:port)/dbname)
	if strings.Contains(s, "@tcp(") || strings.Contains(s, "@unix(") {
		return EngineMySQL, nil
	}
	// Try parsing generic URL scheme
	u, err := url.Parse(s)
	if err == nil && u.Scheme != "" {
		switch strings.ToLower(u.Scheme) {
		case "postgres", "postgresql":
			return EnginePostgres, nil
		case "mysql", "mariadb":
			return EngineMySQL, nil
		}
	}
	return "", fmt.Errorf("unsupported database engine or URI scheme: %q (expected postgres://, postgresql://, mysql://, or mariadb://)", connStr)
}

// ParseMySQLDSN converts a connection string (either mysql:// URL or standard DSN)
// into a driver DSN and the database/schema name.
func ParseMySQLDSN(connStr string) (dsn string, dbName string, err error) {
	s := strings.TrimSpace(connStr)
	if strings.HasPrefix(s, "mysql://") || strings.HasPrefix(s, "mariadb://") {
		u, err := url.Parse(s)
		if err != nil {
			return "", "", fmt.Errorf("parse mysql url: %w", err)
		}
		var user, pass string
		if u.User != nil {
			user = u.User.Username()
			pass, _ = u.User.Password()
		}
		host := u.Host
		if host == "" {
			host = "127.0.0.1:3306"
		} else if !strings.Contains(host, ":") {
			host = host + ":3306"
		}
		dbName = strings.TrimPrefix(u.Path, "/")

		// Query params
		queryParams := u.Query()
		if !queryParams.Has("parseTime") {
			queryParams.Set("parseTime", "true")
		}
		if !queryParams.Has("multiStatements") {
			queryParams.Set("multiStatements", "true")
		}

		userInfo := ""
		if user != "" {
			if pass != "" {
				userInfo = fmt.Sprintf("%s:%s@", user, pass)
			} else {
				userInfo = fmt.Sprintf("%s@", user)
			}
		}
		qStr := queryParams.Encode()
		dsn = fmt.Sprintf("%stcp(%s)/%s", userInfo, host, dbName)
		if qStr != "" {
			dsn += "?" + qStr
		}
		return dsn, dbName, nil
	}

	// Standard DSN pattern: user:pass@tcp(host:port)/dbname?params
	dsn = s
	slashIdx := strings.LastIndex(s, "/")
	if slashIdx != -1 {
		rest := s[slashIdx+1:]
		if qIdx := strings.Index(rest, "?"); qIdx != -1 {
			dbName = rest[:qIdx]
		} else {
			dbName = rest
		}
	}
	return dsn, dbName, nil
}

// QuoteIdent quotes a column or table identifier for the given engine.
func QuoteIdent(engine EngineType, name string) string {
	if engine == EngineMySQL {
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteTable quotes a qualified or unqualified table name for the given engine.
func QuoteTable(engine EngineType, schema, table string) string {
	if schema == "" {
		return QuoteIdent(engine, table)
	}
	return QuoteIdent(engine, schema) + "." + QuoteIdent(engine, table)
}

// RowIterator abstracts over cursor rows for database-agnostic streaming.
type RowIterator interface {
	Next() bool
	Values() ([]any, error)
	Err() error
	Close()
}

// SourceTx represents a live transaction on the source database.
type SourceTx interface {
	Query(ctx context.Context, sql string, args ...any) (RowIterator, error)
	Rollback(ctx context.Context) error
	Commit(ctx context.Context) error
}

// SourceDB provides consistent snapshot acquisition across PostgreSQL and MySQL engines.
type SourceDB interface {
	Engine() EngineType
	BeginSnapshot(ctx context.Context) (SourceTx, error)
	Close(ctx context.Context) error
}

// PostgresSource implements SourceDB for PostgreSQL via pgx.
type PostgresSource struct {
	conn *pgx.Conn
}

// NewPostgresSource wraps a pgx connection as a SourceDB.
func NewPostgresSource(conn *pgx.Conn) *PostgresSource {
	return &PostgresSource{conn: conn}
}

func (p *PostgresSource) Engine() EngineType {
	return EnginePostgres
}

func (p *PostgresSource) BeginSnapshot(ctx context.Context) (SourceTx, error) {
	if p.conn == nil {
		return nil, fmt.Errorf("postgres conn is nil")
	}
	tx, err := p.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	return &postgresSourceTx{tx: tx}, nil
}

func (p *PostgresSource) Close(ctx context.Context) error {
	if p.conn != nil {
		return p.conn.Close(ctx)
	}
	return nil
}

type postgresSourceTx struct {
	tx pgx.Tx
}

func (ptx *postgresSourceTx) PGX() pgx.Tx {
	return ptx.tx
}

func (ptx *postgresSourceTx) Query(ctx context.Context, sql string, args ...any) (RowIterator, error) {
	rows, err := ptx.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (ptx *postgresSourceTx) Rollback(ctx context.Context) error {
	return ptx.tx.Rollback(ctx)
}

func (ptx *postgresSourceTx) Commit(ctx context.Context) error {
	return ptx.tx.Commit(ctx)
}

// MySQLSource implements SourceDB for MySQL 8.0+ and MariaDB via standard database/sql.
type MySQLSource struct {
	db *sql.DB
}

// NewMySQLSource wraps a MySQL sql.DB connection pool as a SourceDB.
func NewMySQLSource(db *sql.DB) *MySQLSource {
	return &MySQLSource{db: db}
}

func (m *MySQLSource) Engine() EngineType {
	return EngineMySQL
}

func (m *MySQLSource) BeginSnapshot(ctx context.Context) (SourceTx, error) {
	if m.db == nil {
		return nil, fmt.Errorf("mysql db is nil")
	}
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire mysql connection: %w", err)
	}

	// 1. NON-LOCKING CONSISTENT SNAPSHOT:
	// "For MySQL sources, begin the extraction session using:
	//  SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	//  START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT;"
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("set mysql isolation level: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT;"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("start mysql consistent snapshot: %w", err)
	}

	return &mysqlSourceTx{conn: conn}, nil
}

func (m *MySQLSource) Close(ctx context.Context) error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

type mysqlSourceTx struct {
	conn *sql.Conn
}

func (mtx *mysqlSourceTx) Query(ctx context.Context, sql string, args ...any) (RowIterator, error) {
	rows, err := mtx.conn.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return NewSQLRowIterator(rows), nil
}

func (mtx *mysqlSourceTx) Rollback(ctx context.Context) error {
	defer mtx.conn.Close()
	_, err := mtx.conn.ExecContext(ctx, "ROLLBACK;")
	return err
}

func (mtx *mysqlSourceTx) Commit(ctx context.Context) error {
	defer mtx.conn.Close()
	_, err := mtx.conn.ExecContext(ctx, "COMMIT;")
	return err
}

// SQLRowIterator wraps standard *sql.Rows to satisfy RowIterator.
type SQLRowIterator struct {
	rows        *sql.Rows
	colNames    []string
	colTypes    []*sql.ColumnType
	initialized bool
	err         error
}

// NewSQLRowIterator wraps standard *sql.Rows as a RowIterator.
func NewSQLRowIterator(rows *sql.Rows) *SQLRowIterator {
	return &SQLRowIterator{rows: rows}
}

func (it *SQLRowIterator) Next() bool {
	if it.rows == nil {
		return false
	}
	return it.rows.Next()
}

func (it *SQLRowIterator) Values() ([]any, error) {
	if !it.initialized {
		var err error
		it.colNames, err = it.rows.Columns()
		if err != nil {
			return nil, err
		}
		it.colTypes, _ = it.rows.ColumnTypes()
		it.initialized = true
	}

	numCols := len(it.colNames)
	dest := make([]any, numCols)
	ptrs := make([]any, numCols)
	for i := range dest {
		ptrs[i] = &dest[i]
	}

	if err := it.rows.Scan(ptrs...); err != nil {
		it.err = err
		return nil, err
	}

	// Normalization for MySQL driver:
	// Convert text/varchar byte slices to string so PII masking and formatting work as expected.
	for i, v := range dest {
		if b, ok := v.([]byte); ok {
			var typeName string
			if i < len(it.colTypes) && it.colTypes[i] != nil {
				typeName = strings.ToLower(it.colTypes[i].DatabaseTypeName())
			}
			if strings.Contains(typeName, "blob") || strings.Contains(typeName, "binary") {
				dest[i] = b
			} else {
				dest[i] = string(b)
			}
		}
	}

	return dest, nil
}

func (it *SQLRowIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	if it.rows != nil {
		return it.rows.Err()
	}
	return nil
}

func (it *SQLRowIterator) Close() {
	if it.rows != nil {
		_ = it.rows.Close()
	}
}
