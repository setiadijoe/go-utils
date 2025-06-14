package querybuilder

import (
	"errors"
	"fmt"
	"strings"
)

// InsertBuilder interface for constructing INSERT queries
type InsertBuilder interface {
	Into(table string) InsertBuilder
	Columns(columns ...string) InsertBuilder
	Values(values ...any) InsertBuilder
	FromSelect(selectBuilder SelectBuilder) InsertBuilder
	OnConflict(conflictAction ConflictAction) InsertBuilder
	Returning(columns ...string) InsertBuilder
	DefaultValues() InsertBuilder
	ToSQL() (string, []any, error)
}

// ConflictAction defines what to do on conflict
type ConflictAction struct {
	Target    string // column or constraint
	DoNothing bool
	DoUpdate  map[string]any
}

// insertBuilder implements InsertBuilder
type insertBuilder struct {
	dialect      Dialect
	table        string
	columns      []string
	values       [][]any
	useDefaults  bool
	fromSelect   SelectBuilder
	conflict     *ConflictAction
	returning    []string
	paramCounter int
}

// Into specifies the table to insert into
func (ib *insertBuilder) Into(table string) InsertBuilder {
	ib.table = table
	return ib
}

// Columns specifies the columns to insert
func (ib *insertBuilder) Columns(columns ...string) InsertBuilder {
	ib.columns = columns
	return ib
}

// Values adds a set of values to insert
func (ib *insertBuilder) Values(values ...any) InsertBuilder {
	ib.values = append(ib.values, values)
	return ib
}

// FromSelect inserts data from a SELECT query
func (ib *insertBuilder) FromSelect(selectBuilder SelectBuilder) InsertBuilder {
	ib.fromSelect = selectBuilder
	return ib
}

// OnConflict specifies conflict resolution
func (ib *insertBuilder) OnConflict(conflictAction ConflictAction) InsertBuilder {
	ib.conflict = &conflictAction
	return ib
}

// Returning specifies columns to return after insert
func (ib *insertBuilder) Returning(columns ...string) InsertBuilder {
	ib.returning = columns
	return ib
}

// DefaultValues specifies to use DEFAULT VALUES clause
func (ib *insertBuilder) DefaultValues() InsertBuilder {
	ib.useDefaults = true
	return ib
}

// ToSQL generates the SQL query and returns the query and parameters
func (ib *insertBuilder) ToSQL() (string, []any, error) {
	if err := ib.validateInsert(); err != nil {
		return "", nil, err
	}

	var (
		query strings.Builder
		args  []interface{}
	)

	query.WriteString("INSERT INTO ")
	query.WriteString(ib.dialect.EscapeIdentifier(ib.table))

	if err := ib.buildColumns(&query); err != nil {
		return "", nil, err
	}

	valArgs, err := ib.buildValuesOrSelectOrDefault(&query)
	if err != nil {
		return "", nil, err
	}
	args = append(args, valArgs...)

	conflictArgs, err := ib.buildOnConflict(&query)
	if err != nil {
		return "", nil, err
	}
	args = append(args, conflictArgs...)

	ib.buildReturning(&query)

	return query.String(), args, nil
}

// validateInsert checks for correct insert configuration
func (ib *insertBuilder) validateInsert() error {
	if ib.table == "" {
		return errors.New("no table specified")
	}

	insertionMethods := 0
	if len(ib.values) > 0 {
		insertionMethods++
	}
	if ib.fromSelect != nil {
		insertionMethods++
	}
	if ib.useDefaults {
		insertionMethods++
	}

	if insertionMethods == 0 {
		return errors.New("no values, select query, or DEFAULT VALUES specified")
	}
	if insertionMethods > 1 {
		return errors.New("cannot specify multiple insertion methods (VALUES, FROM SELECT, DEFAULT VALUES)")
	}

	if len(ib.columns) > 0 && len(ib.values) > 0 {
		for _, valSet := range ib.values {
			if len(valSet) != len(ib.columns) {
				return fmt.Errorf("number of values (%d) doesn't match columns (%d)",
					len(valSet), len(ib.columns))
			}
		}
	}
	return nil
}

// buildColumns writes the columns clause if needed
func (ib *insertBuilder) buildColumns(query *strings.Builder) error {
	if len(ib.columns) > 0 && !ib.useDefaults {
		query.WriteString(" (")
		for i, col := range ib.columns {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString(ib.dialect.EscapeIdentifier(col))
		}
		query.WriteString(")")
	}
	return nil
}

// buildValuesOrSelectOrDefault writes the VALUES, SELECT, or DEFAULT VALUES clause
func (ib *insertBuilder) buildValuesOrSelectOrDefault(query *strings.Builder) ([]interface{}, error) {
	var args []any
	switch {
	case ib.useDefaults:
		query.WriteString(" DEFAULT VALUES")
	case ib.fromSelect != nil:
		query.WriteString(" ")
		selectSQL, selectArgs, err := ib.fromSelect.ToSQL()
		if err != nil {
			return nil, err
		}
		query.WriteString(selectSQL)
		args = append(args, selectArgs...)
	default:
		query.WriteString(" VALUES ")
		for valIdx, valSet := range ib.values {
			if valIdx > 0 {
				query.WriteString(", ")
			}
			query.WriteString("(")
			for i := range valSet {
				if i > 0 {
					query.WriteString(", ")
				}
				query.WriteString(ib.dialect.Placeholder(ib.paramCounter))
				args = append(args, valSet[i])
				ib.paramCounter++
			}
			query.WriteString(")")
		}
	}
	return args, nil
}

// buildOnConflict writes the ON CONFLICT clause if needed
func (ib *insertBuilder) buildOnConflict(query *strings.Builder) ([]interface{}, error) {
	var args []interface{}
	if ib.conflict == nil {
		return args, nil
	}
	query.WriteString(" ON CONFLICT")
	if ib.conflict.Target != "" {
		query.WriteString(" (" + ib.dialect.EscapeIdentifier(ib.conflict.Target) + ")")
	}
	if ib.conflict.DoNothing {
		query.WriteString(" DO NOTHING")
	} else if len(ib.conflict.DoUpdate) > 0 {
		query.WriteString(" DO UPDATE SET ")
		first := true
		for col, val := range ib.conflict.DoUpdate {
			if !first {
				query.WriteString(", ")
			}
			query.WriteString(ib.dialect.EscapeIdentifier(col))
			query.WriteString(" = ")
			query.WriteString(ib.dialect.Placeholder(ib.paramCounter))
			args = append(args, val)
			ib.paramCounter++
			first = false
		}
	}
	return args, nil
}

// buildReturning writes the RETURNING clause if needed
func (ib *insertBuilder) buildReturning(query *strings.Builder) {
	if len(ib.returning) > 0 {
		query.WriteString(" RETURNING ")
		for i, col := range ib.returning {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString(ib.dialect.EscapeIdentifier(col))
		}
	}
}
