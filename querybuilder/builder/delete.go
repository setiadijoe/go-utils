package querybuilder

import (
	"errors"
	"strings"
)

// DeleteBuilder interface for constructing DELETE queries
type DeleteBuilder interface {
	From(table string) DeleteBuilder
	Where(conditions ...Condition) DeleteBuilder
	OrderBy(column string, direction string) DeleteBuilder
	Limit(limit int) DeleteBuilder
	Returning(columns ...string) DeleteBuilder
	ToSQL() (string, []any, error)
}

// deleteBuilder implements DeleteBuilder
type deleteBuilder struct {
	dialect    Dialect
	table      string
	where      []Condition
	orderBy    []order
	limit      *int
	returning  []string
	paramCount int
}

type order struct {
	column    string
	direction string
}

// NewDeleteBuilder creates a new DeleteBuilder instance
func (qb *QueryBuilder) NewDeleteBuilder() DeleteBuilder {
	return &deleteBuilder{
		dialect: qb.dialect,
	}
}

// From specifies the table to delete from
func (db *deleteBuilder) From(table string) DeleteBuilder {
	db.table = table
	return db
}

// Where adds WHERE conditions
func (db *deleteBuilder) Where(conditions ...Condition) DeleteBuilder {
	db.where = append(db.where, conditions...)
	return db
}

// OrderBy adds ORDER BY clause
func (db *deleteBuilder) OrderBy(column string, direction string) DeleteBuilder {
	if direction != "ASC" && direction != "DESC" {
		direction = "ASC"
	}
	db.orderBy = append(db.orderBy, order{
		column:    column,
		direction: direction,
	})
	return db
}

// Limit sets the LIMIT
func (db *deleteBuilder) Limit(limit int) DeleteBuilder {
	db.limit = &limit
	return db
}

// Returning specifies columns to return after delete
func (db *deleteBuilder) Returning(columns ...string) DeleteBuilder {
	db.returning = columns
	return db
}

// ToSQL generates the SQL query and returns the query and parameters
func (db *deleteBuilder) ToSQL() (string, []any, error) {
	if db.table == "" {
		return "", nil, errors.New("no table specified")
	}

	var (
		query strings.Builder
		args  []interface{}
	)

	// DELETE clause
	query.WriteString("DELETE FROM ")
	query.WriteString(db.dialect.EscapeIdentifier(db.table))

	// WHERE clause
	whereSQL, whereArgs := db.buildWhereClause()
	if whereSQL != "" {
		query.WriteString(" WHERE ")
		query.WriteString(whereSQL)
		args = append(args, whereArgs...)
	}

	// ORDER BY clause
	orderBySQL := db.buildOrderByClause()
	if orderBySQL != "" {
		query.WriteString(orderBySQL)
	}

	// LIMIT clause
	limitSQL, limitArgs := db.buildLimitClause()
	if limitSQL != "" {
		query.WriteString(limitSQL)
		args = append(args, limitArgs...)
	}

	// RETURNING clause
	returningSQL := db.buildReturningClause()
	if returningSQL != "" {
		query.WriteString(returningSQL)
	}

	return query.String(), args, nil
}

// buildWhereClause builds the WHERE clause and returns the SQL and arguments.
func (db *deleteBuilder) buildWhereClause() (string, []any) {
	if len(db.where) == 0 {
		return "", nil
	}
	whereSQL, whereArgs := buildConditions(db.where, db.dialect, &db.paramCount)
	return whereSQL, whereArgs
}

// buildOrderByClause builds the ORDER BY clause if supported by the dialect.
func (db *deleteBuilder) buildOrderByClause() string {
	if len(db.orderBy) == 0 {
		return ""
	}
	switch db.dialect.(type) {
	case mysqlDialect, sqliteDialect:
		var sb strings.Builder
		sb.WriteString(" ORDER BY ")
		for i, ob := range db.orderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(db.dialect.EscapeIdentifier(ob.column))
			sb.WriteString(" ")
			sb.WriteString(ob.direction)
		}
		return sb.String()
	default:
		return ""
	}
}

// buildLimitClause builds the LIMIT clause if supported by the dialect.
func (db *deleteBuilder) buildLimitClause() (string, []any) {
	if db.limit == nil {
		return "", nil
	}
	switch db.dialect.(type) {
	case mysqlDialect, sqliteDialect:
		sql := " LIMIT " + db.dialect.Placeholder(db.paramCount)
		args := []any{*db.limit}
		db.paramCount++
		return sql, args
	default:
		return "", nil
	}
}

// buildReturningClause builds the RETURNING clause if supported by the dialect.
func (db *deleteBuilder) buildReturningClause() string {
	if len(db.returning) == 0 {
		return ""
	}
	switch db.dialect.(type) {
	case postgresDialect, sqliteDialect:
		var sb strings.Builder
		sb.WriteString(" RETURNING ")
		for i, col := range db.returning {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(db.dialect.EscapeIdentifier(col))
		}
		return sb.String()
	default:
		return ""
	}
}
