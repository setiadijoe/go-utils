package basic

import (
	"fmt"

	querybuilder "github.com/setiadijoe/go-utils/querybuilder/builder"
)

// How to implement simple basic for sql dialet
func SelectBasic() {
	qb := querybuilder.New().WithDialect(querybuilder.NewMySQLDialect())

	query, args, err := qb.
		Select("id", "name", "age").
		From("people").
		Where(querybuilder.Eq("name", "Alif")).
		ToSQL()
	fmt.Printf("query =====> %s\n", query)
	fmt.Printf("args ======> %+v\n", args...)
	fmt.Println(err)
}
