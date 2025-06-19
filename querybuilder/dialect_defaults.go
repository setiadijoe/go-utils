package querybuilder

import (
	"fmt"
	"strings"
)

// --------------------------
// Base Dialect Implementation
// --------------------------

type baseDialect struct{}

func (d baseDialect) EscapeString(value string) string {
	// Default implementation - should be overridden by specific dialects
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// --------------------------
// MySQL Dialect
// --------------------------

type mysqlDialect struct {
	baseDialect
}

func (d mysqlDialect) Placeholder(index int) string {
	var query strings.Builder
	query.Write([]byte("?"))
	return query.String()
}

// --------------------------
// PostgreSQL Dialect
// --------------------------

type postgresDialect struct {
	baseDialect
}

func (d postgresDialect) Placeholder(index int) string {
	var query strings.Builder
	query.Write([]byte(fmt.Sprintf("$%d", index+1)))
	return query.String()
}

// --------------------------
// SQLite Dialect
// --------------------------

type sqliteDialect struct {
	baseDialect
}

func (d sqliteDialect) Placeholder(index int) string {
	var query strings.Builder
	query.Write([]byte("?"))
	return query.String()
}

// --------------------------
// SQL Server Dialect
// --------------------------

type sqlserverDialect struct {
	baseDialect
}

func (d sqlserverDialect) Placeholder(index int) string {
	var query strings.Builder
	query.Write([]byte(fmt.Sprintf("@p%d", index+1)))
	return query.String()
}

// --------------------------
// Oracle Dialect
// --------------------------

type oracleDialect struct {
	baseDialect
}

func (d oracleDialect) Placeholder(index int) string {
	var query strings.Builder
	query.Write([]byte(fmt.Sprintf(":%d", index+1)))
	return query.String()
}

// --------------------------
// Factory Functions
// --------------------------

func NewMySQLDialect() Dialect {
	return mysqlDialect{}
}

func NewPostgreSQLDialect() Dialect {
	return postgresDialect{}
}

func NewSQLiteDialect() Dialect {
	return sqliteDialect{}
}

func NewSQLServerDialect() Dialect {
	return sqlserverDialect{}
}

func NewOracleDialect() Dialect {
	return oracleDialect{}
}
