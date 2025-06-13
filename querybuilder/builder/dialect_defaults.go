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
	idx := index - 1
	for range idx {
		query.Write([]byte("?, "))
	}
	query.Write([]byte("?"))
	return query.String()
}

func (d mysqlDialect) EscapeIdentifier(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

// MySQL-specific string escaping
func (d mysqlDialect) EscapeString(value string) string {
	// Handle MySQL-specific escaping
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// --------------------------
// PostgreSQL Dialect
// --------------------------

type postgresDialect struct {
	baseDialect
}

func (d postgresDialect) Placeholder(index int) string {
	var query strings.Builder
	idx := index - 1
	for i := range idx {
		query.Write([]byte(fmt.Sprintf("$%d, ", i+1)))
	}
	query.Write([]byte(fmt.Sprintf("$%d", index)))
	return query.String()
}

func (d postgresDialect) EscapeIdentifier(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// PostgreSQL-specific string escaping
func (d postgresDialect) EscapeString(value string) string {
	// PostgreSQL allows dollar-quoted strings
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// --------------------------
// SQLite Dialect
// --------------------------

type sqliteDialect struct {
	baseDialect
}

func (d sqliteDialect) Placeholder(index int) string {
	var query strings.Builder
	idx := index - 1
	for range idx {
		query.Write([]byte("?, "))
	}
	query.Write([]byte("?"))
	return query.String()
}

func (d sqliteDialect) EscapeIdentifier(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// --------------------------
// SQL Server Dialect
// --------------------------

type sqlserverDialect struct {
	baseDialect
}

func (d sqlserverDialect) Placeholder(index int) string {
	var query strings.Builder
	idx := index - 1
	for i := range idx {
		query.Write([]byte(fmt.Sprintf("@p%d, ", i+1)))
	}
	query.Write([]byte(fmt.Sprintf("@p%d", index)))
	return query.String()
}

func (d sqlserverDialect) EscapeIdentifier(ident string) string {
	return "[" + strings.ReplaceAll(ident, "]", "]]") + "]"
}

// SQL Server-specific string escaping
func (d sqlserverDialect) EscapeString(value string) string {
	return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// --------------------------
// Oracle Dialect
// --------------------------

type oracleDialect struct {
	baseDialect
}

func (d oracleDialect) Placeholder(index int) string {
	var query strings.Builder
	idx := index - 1
	for i := range idx {
		query.Write([]byte(fmt.Sprintf(":%d, ", i+1)))
	}
	query.Write([]byte(fmt.Sprintf(":%d", index)))
	return query.String()
}

func (d oracleDialect) EscapeIdentifier(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
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
