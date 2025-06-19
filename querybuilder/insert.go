package querybuilder

import (
	"errors"
	"fmt"
	"regexp"
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

// rawSQL is a helper type for embedding raw SQL expressions in value lists
type rawSQL struct {
	value string
	safe  bool // Mark explicitly safe values}
}

var (
	sqlInjectionRegex = regexp.MustCompile(`(?i)(\bDROP\b|\bDELETE\b|\bINSERT\b|\bUPDATE\b|\bALTER\b)`)
)

// Raw creates a raw SQL expression after basic safety checks
func Raw(value string) any {
	if sqlInjectionRegex.MatchString(value) {
		panic("potentially dangerous raw SQL expression")
	}
	return rawSQL{value: value}
}

// UnsafeRaw explicitly marks raw SQL as safe (use with caution)
func UnsafeRaw(value string) interface{} {
	return rawSQL{value: value, safe: true}
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
	// Convert rawSQL values to proper type
	processedValues := make([]any, len(values))
	for i, v := range values {
		if s, ok := v.(string); ok && strings.HasPrefix(s, "RAW:") {
			processedValues[i] = Raw(strings.TrimPrefix(s, "RAW:"))
		} else {
			processedValues[i] = v
		}
	}

	ib.values = append(ib.values, processedValues)
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
		args  []any
	)

	query.WriteString("INSERT INTO ")
	query.WriteString(ib.table)

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
			query.WriteString(col)
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
			for i, val := range valSet {
				if i > 0 {
					query.WriteString(", ")
				}

				// Handle rawSQL values
				if raw, ok := val.(rawSQL); ok {
					query.WriteString(raw.value)
				} else {
					query.WriteString(ib.dialect.Placeholder(ib.paramCounter))
					args = append(args, val)
					ib.paramCounter++
				}
			}
			query.WriteString(")")
		}
	}

	return args, nil
}

// buildOnConflict writes the ON CONFLICT clause if needed
func (ib *insertBuilder) buildOnConflict(query *strings.Builder) ([]interface{}, error) {
	var args []any
	if ib.conflict == nil {
		return args, nil
	}
	query.WriteString(" ON CONFLICT")
	if ib.conflict.Target != "" {
		query.WriteString(" (" + ib.conflict.Target + ")")
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
			query.WriteString(col)
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
			query.WriteString(col)
		}
	}
}

func (ib *insertBuilder) CurrentTimestamp() any {
	return Raw("CURRENT_TIMESTAMP")
}

func (ib *insertBuilder) Func(funcName string, args ...any) any {
	var parts []string
	var placeholders []string

	for _, arg := range args {
		if raw, ok := arg.(rawSQL); ok {
			parts = append(parts, raw.value)
		} else {
			placeholders = append(placeholders, ib.dialect.Placeholder(ib.paramCounter))
			ib.paramCounter++
		}
	}

	if len(placeholders) > 0 {
		return Raw(fmt.Sprintf("%s(%s)", funcName, strings.Join(placeholders, ",")))
	}
	return Raw(fmt.Sprintf("%s(%s)", funcName, strings.Join(parts, ",")))
}
