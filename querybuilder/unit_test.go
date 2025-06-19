package querybuilder

import (
	"testing"
)

func TestSelect(t *testing.T) {
	tests := []struct {
		name    string
		sb 		SelectBuilder
		isError bool
	}{
		{
			name: "Select Basic MySQL",
			sb:	New().WithDialect(NewMySQLDialect()).Select("id", "full name", "age").From("people").Where(Gt("age", 10)),
		},
		{
			name: "Select Basic MySQL with empty columns",
			sb:   New().WithDialect(NewMySQLDialect()).Select().From("people"),
		},
		{
			name: "Select Basic Postgress",
			sb:   New().WithDialect(NewPostgreSQLDialect()).Select("id", "full name", "age").From("people").Where(Gt("age", 10)),
		},
		{
			name: "Select Basic Oracle",
			sb:   New().WithDialect(NewOracleDialect()).Select("id", "full name", "age").From("people").Where(Gt("age", 10)),
		},
		{
			name: "Select Basic SQLite",
			sb:   New().WithDialect(NewSQLiteDialect()).Select("id", "full name", "age").From("people").Where(Gt("age", 10)),
		},
		{
			name: "Select Basic SQLServer",
			sb:   New().WithDialect(NewSQLServerDialect()).Select("id", "full name", "age").From("people").Where(Gt("age", 10)),
		},
		{
			name: "Select Basic with Join MySQL",
			sb:   New().WithDialect(NewMySQLDialect()).Select("p.id", "p.full_name", "p.age", "o.order_id").
				From("people p").
				Join("orders o", "p.id = o.person_id").
				Where(In("p.age", 10, 11, 22)).
				OrderBy("p.age", "asc").OrderBy("p.full_name", "desc").
				Limit(10).GroupBy("p.id", "p.full_name", "p.age", "o.order_id"),
		},
		{
			name: "Select Basic with Right Join Postgress",
			sb:   New().WithDialect(NewPostgreSQLDialect()).Select("p.id", "p.full_name", "p.age", "o.order_id").
				From("people p").
				RightJoin("orders o", "p.id = o.person_id").
				Where(Like("p.full_name", "%arif")).
				OrderBy("p.age", "ASC").
				Limit(10),
		},
		{
			name: "Select Basic with Left Join Oracle",
			sb:   New().WithDialect(NewOracleDialect()).Select("p.id", "p.full_name", "p.age", "o.order_id").
				From("people p").
				LeftJoin("orders o", "p.id = o.person_id").
				Where(LtOrEq("p.age", 20)).
				OrderBy("p.age", "ASC").
				Limit(10).Offset(10),
		},
		{
			name: "Select Basic with Having Clause SQLite",
			sb:   New().WithDialect(NewSQLiteDialect()).Select("p.id", "p.full_name", "p.age", "COUNT(o.order_id) AS order_count").
				From("people p").Having(Gt("COUNT(o.order_id)", 5)).Distinct(),
		},
		{
			name: "Select Basic with Subquery SQLServer",
			sb:   New().WithDialect(NewSQLServerDialect()).Select("p.id", "p.full_name", "p.age", "o.order_id").FromSubquery(&subquery{
				builder: New().WithDialect(NewSQLServerDialect()).Select("id", "full_name", "age").From("people").Where(Gt("age", 10)),
			}, "p").Join("orders o", "p.id = o.person_id").
				Where(In("p.age", 10, 11, 22)).
				OrderBy("p.age", "asc").
				Limit(10).GroupBy("p.id", "p.full_name", "p.age", "o.order_id"),
		},
		{
			name: "Select with Left Join Subquery MySQL",
			sb:   New().WithDialect(NewMySQLDialect()).Select("p.id", "p.full_name", "p.age", "o.order_id").
				FromSubquery(&subquery{
					builder: New().WithDialect(NewMySQLDialect()).Select("id", "full_name", "age").From("people").Where(Gt("age", 10)),
				}, "p").
				JoinSubquery(&subquery{
					builder: New().WithDialect(NewMySQLDialect()).Select("order_id", "person_id").From("orders"),
				}, "o", "p.id = o.person_id").
				Where(In("p.age", 10, 11, 22)).
				OrderBy("p.age", "asc").
				Limit(10).GroupBy("p.id", "p.full_name", "p.age", "o.order_id"),
		},
		{
			name: "Select with Right Join Subquery Postgress",
			sb:   New().WithDialect(NewPostgreSQLDialect()).Select("p.id", "p.full_name", "p.age", "o.order_id").
				FromSubquery(&subquery{
					builder: New().WithDialect(NewPostgreSQLDialect()).Select("id", "full_name", "age").From("people").Where(Gt("age", 10)),
				}, "p").
				RightJoinSubquery(&subquery{
					builder: New().WithDialect(NewPostgreSQLDialect()).Select("order_id", "person_id").From("orders"),
				}, "o", "p.id = o.person_id").
				Where(Like("p.full_name", "%arif")).
				OrderBy("p.age", "ASC").
				Limit(10),
		},
		{
			name: "Select with Left Join Subquery Oracle",
			sb:   New().WithDialect(NewOracleDialect()).Select("p.id", "p.full_name", "p.age", "o.order_id").
				FromSubquery(&subquery{
					builder: New().WithDialect(NewOracleDialect()).Select("id", "full_name", "age").From("people").Where(Gt("age", 10)),
				}, "p").
				LeftJoinSubquery(&subquery{
					builder: New().WithDialect(NewOracleDialect()).Select("order_id", "person_id").From("orders"),
				}, "o", "p.id = o.person_id").
				Where(LtOrEq("p.age", 20)).
				OrderBy("p.age", "ASC").
				Limit(10).Offset(10),
		},
		{
			name: "Select with table is nil MySQL",
			sb:   New().WithDialect(NewMySQLDialect()).Select("id", "full_name", "age").From("").Where(Gt("age", 10)),
			isError: true,
		},
		{
			name: "Select with table in subquery is nil MySQL",
			sb:   New().WithDialect(NewMySQLDialect()).Select("id", "full_name", "age").FromSubquery(&subquery{
				builder: New().WithDialect(NewMySQLDialect()).Select("id", "full_name", "age").From("people").Where(Gt("age", 10)),
			}, "p").JoinSubquery(&subquery{
				builder: New().WithDialect(NewMySQLDialect()).Select("order_id", "person_id"),
			}, "o", "p.id = o.person_id"),
			isError: true,
		},
		{
			name: "Select with table in subquery FRPM is nil MySQL",
			sb:   New().WithDialect(NewMySQLDialect()).Select("id", "full_name", "age").FromSubquery(&subquery{
				builder: New().WithDialect(NewMySQLDialect()).Select("id", "full_name", "age").Where(Gt("age", 10)),
			}, "p"),
			isError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := tt.sb.ToSQL()
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
		ib 	InsertBuilder
		isError bool
	}{
		{
			name: "Insert MySQL",
			ib:   New().WithDialect(NewMySQLDialect()).Insert("people").Columns("id", "full name", "age", "is_healthy").Values(1, "Arif", 10, false), 
		},
		{
			name: "Insert Postgress",
			ib:   New().WithDialect(NewPostgreSQLDialect()).Insert("people").Columns("id", "full name", "age", "is_healthy").Values(1, "Arif", 10, false),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := tt.ib.ToSQL()
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
		ub 	UpdateBuilder
		isError bool
	}{
		{
			name: "Update MySQL",
			ub:   New().WithDialect(NewMySQLDialect()).Update("people").SetValues(map[string]any{
				"fullname":   "Arif Setiawan",
				"occupation": "Software Engineer",
			}).Where(Eq("id", 1)),
		},
		{
			name: "Update Postgress",
			ub:   New().WithDialect(NewPostgreSQLDialect()).Update("people").SetValues(map[string]any{
				"fullname":   "Arif Setiawan",
				"occupation": "Software Engineer",
			}).Where(Eq("id", 1)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := tt.ub.ToSQL()
			if tt.isError && err == nil {
				t.Error("Should return error")
			} else {
				t.Logf("query ==========> %s ------- arguments ==========> %+v", query, args)
			}
		})
	}
}

func TestDeleteBasic(t *testing.T) {
	tests := []struct {
		name    string
		db DeleteBuilder
		isError bool
	}{
		{
			name: "Delete MySQL",
			db:   New().WithDialect(NewMySQLDialect()).Delete("people").Where(Eq("id", 1)),
		},
		{
			name: "Delete Postgress",
			db:   New().WithDialect(NewPostgreSQLDialect()).Delete("people").Where(Eq("id", 1)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := tt.db.ToSQL()
			if tt.isError && err == nil {
				t.Error("Should return error")
			} else {
				t.Logf("query ==========> %s ------- arguments ==========> %+v", query, args)
			}
		})
	}
}
