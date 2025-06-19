package basic

import (
	"testing"

	querybuilder "github.com/setiadijoe/go-utils/querybuilder"
)

func TestSelectBasic(t *testing.T) {
	tests := []struct {
		name    string
		qb      querybuilder.Builder
		isError bool
	}{
		{
			name: "Select Basic MySQL",
			qb:   querybuilder.New().WithDialect(querybuilder.NewMySQLDialect()),
		},
		{
			name: "Select Basic Postgress",
			qb:   querybuilder.New().WithDialect(querybuilder.NewPostgreSQLDialect()),
		},
		{
			name: "Select Basic Oracle",
			qb:   querybuilder.New().WithDialect(querybuilder.NewOracleDialect()),
		},
		{
			name: "Select Basic SQLite",
			qb:   querybuilder.New().WithDialect(querybuilder.NewSQLiteDialect()),
		},
		{
			name: "Select Basic SQLServer",
			qb:   querybuilder.New().WithDialect(querybuilder.NewSQLServerDialect()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := tt.qb.Select("id", "full name", "age").From("people").Where(querybuilder.Gt("age", 10)).ToSQL()
			if tt.isError && err == nil {
				t.Error("should return error")
			} else {
				t.Logf("query ===> %s  ====> arguments =====> %+v", query, args)
			}
		})
	}
}

func TestInsertSingleBasic(t *testing.T) {
	tests := []struct {
		name    string
		qb      querybuilder.Builder
		isError bool
	}{
		{
			name: "Insert MySQL",
			qb:   querybuilder.New().WithDialect(querybuilder.NewMySQLDialect()),
		},
		{
			name: "Insert Postgress",
			qb:   querybuilder.New().WithDialect(querybuilder.NewPostgreSQLDialect()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := tt.qb.Insert("people").Columns("id", "full name", "age", "is_healthy").Values(1, "Arif", 10, false).ToSQL()
			if tt.isError && err == nil {
				t.Error("Should return error")
			} else {
				t.Logf("query ==========> %s ------- arguments ==========> %+v", query, args)
			}
		})
	}
}

func TestUpdateBasic(t *testing.T) {
		tests := []struct {
		name    string
		qb      querybuilder.Builder
		isError bool
	}{
		{
			name: "Update MySQL",
			qb:   querybuilder.New().WithDialect(querybuilder.NewMySQLDialect()),
		},
		{
			name: "Update Postgress",
			qb:   querybuilder.New().WithDialect(querybuilder.NewPostgreSQLDialect()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := tt.qb.Update("people").SetValues(map[string]any{
				"fullname": "Arif Setiawan",
				"occupation": "Software Engineer",
			}).Where(querybuilder.Eq("id", 1)).ToSQL()
			if tt.isError && err == nil {
				t.Error("Should return error")
			} else {
				t.Logf("query ==========> %s ------- arguments ==========> %+v", query, args)
			}
		})
	}
}