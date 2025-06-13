package querybuilder

import (
	"fmt"
	"strings"
)

// Condition represents a SQL WHERE condition
type Condition interface {
	ToSQL(dialect Dialect, argPos *int) (string, []any)
}

// Operator represents comparison operators
type Operator string

const (
	Equal            Operator = "="
	NotEqual         Operator = "<>"
	GreatThan        Operator = ">"
	GreatThanOrEqual Operator = ">="
	LessTnan         Operator = "<"
	LessThanOrEqual  Operator = "<="
	LikeOp           Operator = "LIKE"
	NotLikeOp        Operator = "NOT LIKE"
	InOp             Operator = "IN"
	NotInOp          Operator = "NOT IN"
	IsNullOp         Operator = "IS NULL"
	IsNotNullOp      Operator = "IS NOT NULL"
	BetweenOp        Operator = "BETWEEN"
)

// baseCondition implements common condition functionality
type baseCondition struct {
	column    string
	operator  Operator
	value     any
	valueType string // "value", "column", "subquery"
}

// ToSQL converts the condition to SQL with proper escaping
func (c *baseCondition) ToSQL(dialect Dialect, argPos *int) (string, []any) {
	var (
		sql  strings.Builder
		args []any
	)

	// Column identifier
	sql.WriteString(dialect.EscapeIdentifier(c.column))
	sql.WriteString(" ")
	sql.WriteString(string(c.operator))

	// Handle NULL checks specially
	if c.operator == IsNullOp || c.operator == IsNotNullOp {
		return sql.String(), nil
	}

	sql.WriteString(" ")

	switch c.valueType {
	case "column":
		sql.WriteString(dialect.EscapeIdentifier(c.value.(string)))
	case "subquery":
		subquery, subArgs, _ := c.value.(SQLBuilder).ToSQL()
		sql.WriteString("(")
		sql.WriteString(subquery)
		sql.WriteString(")")
		args = append(args, subArgs...)
	default:
		// Regular value
		sql.WriteString(dialect.Placeholder(*argPos))
		args = append(args, c.value)
		*argPos++
	}

	return sql.String(), args
}

// NewCondition creates a new base condition
func newCondition(column string, operator Operator, value any, valueType string) Condition {
	return &baseCondition{
		column:    column,
		operator:  operator,
		value:     value,
		valueType: valueType,
	}
}

// Eq creates an equality condition
func Eq(column string, value interface{}) Condition {
	return newCondition(column, Equal, value, "value")
}

// NotEq creates an inequality condition
func NotEq(column string, value interface{}) Condition {
	return newCondition(column, NotEqual, value, "value")
}

// Gt creates a greater-than condition
func Gt(column string, value interface{}) Condition {
	return newCondition(column, GreatThan, value, "value")
}

// GtOrEq creates a greater-than-or-equal condition
func GtOrEq(column string, value interface{}) Condition {
	return newCondition(column, GreatThanOrEqual, value, "value")
}

// Lt creates a less-than condition
func Lt(column string, value interface{}) Condition {
	return newCondition(column, LessTnan, value, "value")
}

// LtOrEq creates a less-than-or-equal condition
func LtOrEq(column string, value interface{}) Condition {
	return newCondition(column, LessThanOrEqual, value, "value")
}

// Like creates a LIKE condition
func Like(column string, pattern interface{}) Condition {
	return newCondition(column, LikeOp, pattern, "value")
}

// NotLike creates a NOT LIKE condition
func NotLike(column string, pattern interface{}) Condition {
	return newCondition(column, NotLikeOp, pattern, "value")
}

// In creates an IN condition
func In(column string, values ...interface{}) Condition {
	return newCondition(column, InOp, values, "value")
}

// NotIn creates a NOT IN condition
func NotIn(column string, values ...interface{}) Condition {
	return newCondition(column, NotInOp, values, "value")
}

// IsNull creates an IS NULL condition
func IsNull(column string) Condition {
	return newCondition(column, IsNullOp, nil, "value")
}

// IsNotNull creates an IS NOT NULL condition
func IsNotNull(column string) Condition {
	return newCondition(column, IsNotNullOp, nil, "value")
}

// Between creates a BETWEEN condition
func Between(column string, from, to interface{}) Condition {
	return &betweenCondition{
		column: column,
		from:   from,
		to:     to,
	}
}

// ColumnEq creates a column equality condition
func ColumnEq(column1, column2 string) Condition {
	return newCondition(column1, Equal, column2, "column")
}

// betweenCondition handles BETWEEN expressions
type betweenCondition struct {
	column string
	from   any
	to     any
}

func (c *betweenCondition) ToSQL(dialect Dialect, argPos *int) (string, []any) {
	var (
		sql  strings.Builder
		args []any
	)

	sql.WriteString(dialect.EscapeIdentifier(c.column))
	sql.WriteString(" BETWEEN ")
	sql.WriteString(dialect.Placeholder(*argPos))
	args = append(args, c.from)
	*argPos++

	sql.WriteString(" AND ")
	sql.WriteString(dialect.Placeholder(*argPos))
	args = append(args, c.to)
	*argPos++

	return sql.String(), args
}

// And combines conditions with AND
func And(conditions ...Condition) Condition {
	return &logicalCondition{
		operator:   "AND",
		conditions: conditions,
	}
}

// Or combines conditions with OR
func Or(conditions ...Condition) Condition {
	return &logicalCondition{
		operator:   "OR",
		conditions: conditions,
	}
}

// logicalCondition handles AND/OR groups
type logicalCondition struct {
	operator   string
	conditions []Condition
}

func (c *logicalCondition) ToSQL(dialect Dialect, argPos *int) (string, []any) {
	if len(c.conditions) == 0 {
		return "", nil
	}

	var (
		sql     strings.Builder
		parts   []string
		allArgs []any
	)

	for _, cond := range c.conditions {
		partSQL, partArgs := cond.ToSQL(dialect, argPos)
		parts = append(parts, partSQL)
		allArgs = append(allArgs, partArgs...)
	}

	if len(parts) > 1 {
		sql.WriteString("(")
	}

	sql.WriteString(strings.Join(parts, fmt.Sprintf(" %s ", c.operator)))

	if len(parts) > 1 {
		sql.WriteString(")")
	}

	return sql.String(), allArgs
}
