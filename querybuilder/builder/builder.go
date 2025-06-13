package querybuilder

type Builder interface {
	Select(columns ...string) SelectBuilder
	Insert(table string) InsertBuilder
	Update(table string) UpdateBuilder
	Delete(table string) DeleteBuilder
	WithDialect(dialect Dialect) Builder
}

type SQLBuilder interface {
	ToSQL() (string, []any, error)
}

// Dialect defines database-specific SQL generation rules
type Dialect interface {
	Placeholder(index int) string
	EscapeIdentifier(ident string) string
	EscapeString(value string) string
}

// QueryBuilder is the concrete implementation of Builder
type QueryBuilder struct {
	dialect Dialect
}

// New creates a new QueryBuilder instance
func New(dialect Dialect) *QueryBuilder {
	return &QueryBuilder{
		dialect: dialect,
	}
}

// WithDialect sets the SQL dialect for the builder
func (qb *QueryBuilder) WithDialect(dialect Dialect) Builder {
	qb.dialect = dialect
	return qb
}

// Select begins a SELECT query
func (qb *QueryBuilder) Select(columns ...string) SelectBuilder {
	return &selectBuilder{
		columns:  columns,
		dialect:  qb.dialect,
		distinct: false,
	}
}

// Insert begins a INSERT query
func (qb *QueryBuilder) Insert(table string) InsertBuilder {
	return &insertBuilder{
		table:   table,
		dialect: qb.dialect,
	}
}

// Update begins an UPDATE query
func (qb *QueryBuilder) Update(table string) UpdateBuilder {
	return &updateBuilder{
		table:   table,
		dialect: qb.dialect,
	}
}

// Delete begins a DELETE query
func (qb *QueryBuilder) Delete(table string) DeleteBuilder {
	return &deleteBuilder{
		table:   table,
		dialect: qb.dialect,
	}
}

// Basic condition implementation
type basicCondition struct {
	column    string
	operator  string
	value     any
	valueType string // "column", "value", "subquery"
}
