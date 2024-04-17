package test

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"testing"
)

func TestListTables(t *testing.T) {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", "fgseitsrancher.wifa.intern.uni-leipzig.de",
		5432, "postgres", "tea", "postgres")
	log.Println("Connecting to PSQL...", psqlconn)
	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		t.Fatal(err.Error())
	}
	res, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_name like 'device:PfoCsJwASh2N2NNNqVGtDg%';")
	if err != nil {
		t.Fatal(err.Error())
	}
	for res.Next() {
		var table []byte
		err = res.Scan(&table)
		if err != nil {
			t.Fatal(err.Error())
		}

		log.Println("Would delete table", string(table))
	}
	err = res.Err()
	if err != nil {
		t.Fatal(err.Error())
	}



	fmt.Sprintf("%v", res)
}
