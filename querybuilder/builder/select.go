package builder

import (
	"errors"
	"fmt"
	"strings"
)

// SelectBuilder interface for chaining SELECT operations
type SelectBuilder interface {
	From(table string) SelectBuilder
	Where(conditions ...Condition) SelectBuilder
	Join(table, on string) SelectBuilder
	LeftJoin(table, on string) SelectBuilder
	RightJoin(table, on string) SelectBuilder
	GroupBy(columns ...string) SelectBuilder
	Having(conditions ...Condition) SelectBuilder
	OrderBy(column string, direction string) SelectBuilder
	Limit(limit int) SelectBuilder
	Offset(offset int) SelectBuilder
	Distinct() SelectBuilder
	ToSQL() (string, []any, error)
}

// selectBuilder implements SelectBuilder
type selectBuilder struct {
	dialect    Dialect
	distinct   bool
	columns    []string
	table      string
	joins      []join
	where      []Condition
	groupBy    []string
	having     []Condition
	orderBy    []order
	limit      *int
	offset     *int
	args       []any
	paramCount int
	subquery   *subquery
}

// Subquery represents a subquery in FROM or JOIN clauses
type Subquery interface {
	SQLBuilder
	As(alias string) Subquery
}

type join struct {
	joinType  string
	table     string
	subquery  *subquery
	condition string
}

type order struct {
	column    string
	direction string // "ASC", "DESC"
}

// NewSelectBuilder creates a new SelectBuilder instance
func (qb *QueryBuilder) newSelectBuilder(columns ...string) *selectBuilder {
	return &selectBuilder{
		dialect:  qb.dialect,
		columns:  columns,
		distinct: false,
	}
}

// From specifies the table to select from
func (sb *selectBuilder) From(table string) SelectBuilder {
	sb.table = table
	return sb
}

// Where adds WHERE conditions
func (sb *selectBuilder) Where(conditions ...Condition) SelectBuilder {
	sb.where = append(sb.where, conditions...)
	return sb
}

// Join adds an INNER JOIN
func (sb *selectBuilder) Join(table, on string) SelectBuilder {
	sb.joins = append(sb.joins, join{
		joinType:  "INNER",
		table:     table,
		condition: on,
	})
	return sb
}

// LeftJoin adds a LEFT JOIN
func (sb *selectBuilder) LeftJoin(table, on string) SelectBuilder {
	sb.joins = append(sb.joins, join{
		joinType:  "LEFT",
		table:     table,
		condition: on,
	})
	return sb
}

// RightJoin adds a RIGHT JOIN
func (sb *selectBuilder) RightJoin(table, on string) SelectBuilder {
	sb.joins = append(sb.joins, join{
		joinType:  "RIGHT",
		table:     table,
		condition: on,
	})
	return sb
}

// GroupBy adds GROUP BY columns
func (sb *selectBuilder) GroupBy(columns ...string) SelectBuilder {
	sb.groupBy = append(sb.groupBy, columns...)
	return sb
}

// Having adds HAVING conditions
func (sb *selectBuilder) Having(conditions ...Condition) SelectBuilder {
	sb.having = append(sb.having, conditions...)
	return sb
}

// OrderBy adds ORDER BY clause
func (sb *selectBuilder) OrderBy(column string, direction string) SelectBuilder {
	if direction != "ASC" && direction != "DESC" {
		direction = "ASC"
	}
	sb.orderBy = append(sb.orderBy, order{
		column:    column,
		direction: direction,
	})
	return sb
}

// Limit sets the LIMIT
func (sb *selectBuilder) Limit(limit int) SelectBuilder {
	sb.limit = &limit
	return sb
}

// Offset sets the OFFSET
func (sb *selectBuilder) Offset(offset int) SelectBuilder {
	sb.offset = &offset
	return sb
}

// Distinct sets the DISTINCT flag
func (sb *selectBuilder) Distinct() SelectBuilder {
	sb.distinct = true
	return sb
}

// ToSQL generates the SQL query and returns the query and parameters
func (sb *selectBuilder) ToSQL() (string, []any, error) {
	if sb.table == "" && sb.subquery == nil {
		return "", nil, errors.New("no table or subquery specified for FROM clause")
	}

	var (
		query strings.Builder
		args  []any
	)

	// SELECT clause
	if err := sb.buildSelectClause(&query); err != nil {
		return "", nil, err
	}

	// FROM clause
	fromArgs, err := sb.buildFromClause(&query)
	if err != nil {
		return "", nil, err
	}
	args = append(args, fromArgs...)

	// JOIN clauses
	joinArgs, err := sb.buildJoinClauses(&query)
	if err != nil {
		return "", nil, err
	}
	args = append(args, joinArgs...)

	// WHERE clause
	whereArgs, err := sb.buildWhereClause(&query)
	if err != nil {
		return "", nil, err
	}
	args = append(args, whereArgs...)

	// GROUP BY clause
	groupByArgs, err := sb.buildGroupByClause(&query)
	if err != nil {
		return "", nil, err
	}
	args = append(args, groupByArgs...)

	// HAVING clause
	havingArgs, err := sb.buildHavingClause(&query)
	if err != nil {
		return "", nil, err
	}
	args = append(args, havingArgs...)

	// ORDER BY clause
	if err := sb.buildOrderByClause(&query); err != nil {
		return "", nil, err
	}

	// LIMIT clause
	limitArgs := sb.buildLimitClause(&query)
	args = append(args, limitArgs...)

	// OFFSET clause
	offsetArgs := sb.buildOffsetClause(&query)
	args = append(args, offsetArgs...)

	return query.String(), args, nil
}

// buildSelectClause builds the SELECT clause.
func (sb *selectBuilder) buildSelectClause(query *strings.Builder) error {
	query.WriteString("SELECT ")
	if sb.distinct {
		query.WriteString("DISTINCT ")
	}
	if len(sb.columns) == 0 {
		query.WriteString("*")
	} else {
		for i, col := range sb.columns {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString(sb.dialect.EscapeIdentifier(col))
		}
	}
	return nil
}

// buildFromClause builds the FROM clause and returns its args.
func (sb *selectBuilder) buildFromClause(query *strings.Builder) ([]any, error) {
	var args []any
	query.WriteString(" FROM ")
	if sb.subquery != nil {
		subSQL, subArgs, err := sb.subquery.ToSQL()
		if err != nil {
			return nil, err
		}
		query.WriteString(subSQL)
		if sb.subquery.alias != "" {
			query.WriteString(" AS ")
			query.WriteString(sb.dialect.EscapeIdentifier(sb.subquery.alias))
		}
		args = append(args, subArgs...)
	} else {
		query.WriteString(sb.dialect.EscapeIdentifier(sb.table))
	}
	return args, nil
}

// buildJoinClauses builds JOIN clauses and returns their args.
func (sb *selectBuilder) buildJoinClauses(query *strings.Builder) ([]any, error) {
	var args []any
	for _, j := range sb.joins {
		query.WriteString(fmt.Sprintf(" %s JOIN ", j.joinType))
		if j.subquery != nil {
			subSQL, subArgs, err := j.subquery.ToSQL()
			if err != nil {
				return nil, err
			}
			query.WriteString(subSQL)
			if j.subquery.alias != "" {
				query.WriteString(" AS ")
				query.WriteString(sb.dialect.EscapeIdentifier(j.subquery.alias))
			}
			args = append(args, subArgs...)
		} else {
			query.WriteString(sb.dialect.EscapeIdentifier(j.table))
		}
		query.WriteString(" ON ")
		query.WriteString(j.condition)
	}
	return args, nil
}

// buildWhereClause builds the WHERE clause and returns its args.
func (sb *selectBuilder) buildWhereClause(query *strings.Builder) ([]any, error) {
	if len(sb.where) == 0 {
		return nil, nil
	}
	whereSQL, whereArgs := buildConditions(sb.where, sb.dialect, &sb.paramCount)
	query.WriteString(" WHERE ")
	query.WriteString(whereSQL)
	return whereArgs, nil
}

// buildGroupByClause builds the GROUP BY clause and returns its args.
func (sb *selectBuilder) buildGroupByClause(query *strings.Builder) ([]any, error) {
	if len(sb.groupBy) == 0 {
		return nil, nil
	}
	query.WriteString(" GROUP BY ")
	for i, col := range sb.groupBy {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(sb.dialect.EscapeIdentifier(col))
	}
	return nil, nil
}

// buildHavingClause builds the HAVING clause and returns its args.
func (sb *selectBuilder) buildHavingClause(query *strings.Builder) ([]any, error) {
	if len(sb.having) == 0 {
		return nil, nil
	}
	havingSQL, havingArgs := buildConditions(sb.having, sb.dialect, &sb.paramCount)
	query.WriteString(" HAVING ")
	query.WriteString(havingSQL)
	return havingArgs, nil
}

// buildOrderByClause builds the ORDER BY clause.
func (sb *selectBuilder) buildOrderByClause(query *strings.Builder) error {
	if len(sb.orderBy) == 0 {
		return nil
	}
	query.WriteString(" ORDER BY ")
	for i, ob := range sb.orderBy {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(sb.dialect.EscapeIdentifier(ob.column))
		query.WriteString(" ")
		query.WriteString(ob.direction)
	}
	return nil
}

func (sb *selectBuilder) buildLimitClause(query *strings.Builder) []any {
	if sb.limit == nil {
		return nil
	}
	query.WriteString(" LIMIT ")
	query.WriteString(sb.dialect.Placeholder(sb.paramCount))
	sb.paramCount++
	return []any{*sb.limit}
}

func (sb *selectBuilder) buildOffsetClause(query *strings.Builder) []any {
	if sb.offset == nil {
		return nil
	}
	query.WriteString(" OFFSET ")
	query.WriteString(sb.dialect.Placeholder(sb.paramCount))
	sb.paramCount++
	return []any{*sb.offset}
}

// Helper function to build conditions
func buildConditions(conditions []Condition, dialect Dialect, paramCount *int) (string, []any) {
	var (
		sqlParts []string
		args     []any
	)

	for _, cond := range conditions {
		sql, condArgs := cond.ToSQL(dialect, paramCount)
		sqlParts = append(sqlParts, sql)
		args = append(args, condArgs...)
	}

	return strings.Join(sqlParts, " AND "), args
}

// subquery implements Subquery
type subquery struct {
	builder SQLBuilder
	alias   string
}

// As sets the alias for the subquery
func (s *subquery) As(alias string) Subquery {
	s.alias = alias
	return s
}

// ToSQL generates the subquery SQL
func (s *subquery) ToSQL() (string, []any, error) {
	sql, args, err := s.builder.ToSQL()
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("(%s)", sql), args, nil
}

// FromSubquery creates a FROM clause with a subquery
func (sb *selectBuilder) FromSubquery(subq SQLBuilder, alias string) SelectBuilder {
	sb.table = ""
	sb.subquery = &subquery{
		builder: subq,
		alias:   alias,
	}
	return sb
}

// JoinSubquery adds a JOIN with a subquery
func (sb *selectBuilder) JoinSubquery(subq SQLBuilder, alias, on string) SelectBuilder {
	return sb.joinSubquery("INNER", subq, alias, on)
}

// LeftJoinSubquery adds a LEFT JOIN with a subquery
func (sb *selectBuilder) LeftJoinSubquery(subq SQLBuilder, alias, on string) SelectBuilder {
	return sb.joinSubquery("LEFT", subq, alias, on)
}

// RightJoinSubquery adds a RIGHT JOIN with a subquery
func (sb *selectBuilder) RightJoinSubquery(subq SQLBuilder, alias, on string) SelectBuilder {
	return sb.joinSubquery("RIGHT", subq, alias, on)
}

func (sb *selectBuilder) joinSubquery(joinType string, subq SQLBuilder, alias, on string) SelectBuilder {
	sb.joins = append(sb.joins, join{
		joinType:  joinType,
		subquery:  &subquery{builder: subq, alias: alias},
		condition: on,
	})
	return sb
}
