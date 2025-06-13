package basic

import (
	"testing"

	querybuilder "github.com/setiadijoe/go-utils/querybuilder/builder"
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
