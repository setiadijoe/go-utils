package querybuilder

import (
	"errors"
	"strings"
)

// UpdateBuilder interface for constructing UPDATE queries
type UpdateBuilder interface {
	Table(table string) UpdateBuilder
	Set(column string, value interface{}) UpdateBuilder
	SetRaw(column string, expression string) UpdateBuilder
	Where(conditions ...Condition) UpdateBuilder
	OrderBy(column string, direction string) UpdateBuilder
	Limit(limit int) UpdateBuilder
	Returning(columns ...string) UpdateBuilder
	ToSQL() (string, []interface{}, error)
}

// updateBuilder implements UpdateBuilder
type updateBuilder struct {
	dialect    Dialect
	table      string
	sets       []setClause
	where      []Condition
	orderBy    []order
	limit      *int
	returning  []string
	paramCount int
}

type setClause struct {
	column string
	value  any
	isRaw  bool
}

// NewUpdateBuilder creates a new UpdateBuilder instance
func (qb *QueryBuilder) NewUpdateBuilder() UpdateBuilder {
	return &updateBuilder{
		dialect: qb.dialect,
		sets:    make([]setClause, 0),
	}
}

// Table specifies the table to update
func (ub *updateBuilder) Table(table string) UpdateBuilder {
	ub.table = table
	return ub
}

// Set adds a column-value pair to update
func (ub *updateBuilder) Set(column string, value interface{}) UpdateBuilder {
	ub.sets = append(ub.sets, setClause{
		column: column,
		value:  value,
		isRaw:  false,
	})
	return ub
}

// SetRaw adds a column with raw SQL expression to update
func (ub *updateBuilder) SetRaw(column string, expression string) UpdateBuilder {
	ub.sets = append(ub.sets, setClause{
		column: column,
		value:  expression,
		isRaw:  true,
	})
	return ub
}

// Where adds WHERE conditions
func (ub *updateBuilder) Where(conditions ...Condition) UpdateBuilder {
	ub.where = append(ub.where, conditions...)
	return ub
}

// OrderBy adds ORDER BY clause
func (ub *updateBuilder) OrderBy(column string, direction string) UpdateBuilder {
	if direction != "ASC" && direction != "DESC" {
		direction = "ASC"
	}
	ub.orderBy = append(ub.orderBy, order{
		column:    column,
		direction: direction,
	})
	return ub
}

// Limit sets the LIMIT
func (ub *updateBuilder) Limit(limit int) UpdateBuilder {
	ub.limit = &limit
	return ub
}

// Returning specifies columns to return after update
func (ub *updateBuilder) Returning(columns ...string) UpdateBuilder {
	ub.returning = columns
	return ub
}

// ToSQL generates the SQL query and returns the query and parameters
func (ub *updateBuilder) ToSQL() (string, []any, error) {
	if ub.table == "" {
		return "", nil, errors.New("no table specified")
	}

	if len(ub.sets) == 0 {
		return "", nil, errors.New("no set values specified")
	}

	var (
		query strings.Builder
		args  []interface{}
	)

	query.WriteString("UPDATE ")
	query.WriteString(ub.dialect.EscapeIdentifier(ub.table))

	setClause, setArgs := ub.buildSetClause()
	query.WriteString(setClause)
	args = append(args, setArgs...)

	whereClause, whereArgs := ub.buildWhereClause()
	query.WriteString(whereClause)
	args = append(args, whereArgs...)

	orderByClause := ub.buildOrderByClause()
	query.WriteString(orderByClause)

	limitClause, limitArgs := ub.buildLimitClause()
	query.WriteString(limitClause)
	args = append(args, limitArgs...)

	returningClause := ub.buildReturningClause()
	query.WriteString(returningClause)

	return query.String(), args, nil
}

// buildSetClause builds the SET clause and returns the clause and its arguments.
func (ub *updateBuilder) buildSetClause() (string, []any) {
	var clause strings.Builder
	var args []any
	clause.WriteString(" SET ")
	for i, set := range ub.sets {
		if i > 0 {
			clause.WriteString(", ")
		}
		clause.WriteString(ub.dialect.EscapeIdentifier(set.column))
		clause.WriteString(" = ")
		if set.isRaw {
			clause.WriteString(set.value.(string))
		} else {
			clause.WriteString(ub.dialect.Placeholder(ub.paramCount))
			args = append(args, set.value)
			ub.paramCount++
		}
	}
	return clause.String(), args
}

// buildWhereClause builds the WHERE clause and returns the clause and its arguments.
func (ub *updateBuilder) buildWhereClause() (string, []any) {
	if len(ub.where) == 0 {
		return "", nil
	}
	whereSQL, whereArgs := buildConditions(ub.where, ub.dialect, &ub.paramCount)
	return " WHERE " + whereSQL, whereArgs
}

// buildOrderByClause builds the ORDER BY clause.
func (ub *updateBuilder) buildOrderByClause() string {
	if len(ub.orderBy) == 0 {
		return ""
	}
	var clause strings.Builder
	clause.WriteString(" ORDER BY ")
	for i, ob := range ub.orderBy {
		if i > 0 {
			clause.WriteString(", ")
		}
		clause.WriteString(ub.dialect.EscapeIdentifier(ob.column))
		clause.WriteString(" ")
		clause.WriteString(ob.direction)
	}
	return clause.String()
}

// buildLimitClause builds the LIMIT clause and returns the clause and its arguments.
func (ub *updateBuilder) buildLimitClause() (string, []any) {
	if ub.limit == nil {
		return "", nil
	}
	switch ub.dialect.(type) {
	case mysqlDialect, sqliteDialect:
		clause := " LIMIT " + ub.dialect.Placeholder(ub.paramCount)
		args := []any{*ub.limit}
		ub.paramCount++
		return clause, args
	default:
		return "", nil
	}
}

// buildReturningClause builds the RETURNING clause.
func (ub *updateBuilder) buildReturningClause() string {
	if len(ub.returning) == 0 {
		return ""
	}
	switch ub.dialect.(type) {
	case postgresDialect, sqliteDialect:
		var clause strings.Builder
		clause.WriteString(" RETURNING ")
		for i, col := range ub.returning {
			if i > 0 {
				clause.WriteString(", ")
			}
			clause.WriteString(ub.dialect.EscapeIdentifier(col))
		}
		return clause.String()
	default:
		return ""
	}
}
