/*
 * Copyright 2020 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package handler

import (
	"database/sql"
	"log"
)

func (handler *Handler) migrate() error {
	return handler.migrateTIMESTAMP_TIMESTAMPTZ()
}

func (handler *Handler) migrateTIMESTAMP_TIMESTAMPTZ() error {
	i := 0
	rows, err := handler.db.Query("SELECT distinct(table_name) FROM information_schema.columns WHERE table_schema = 'public' AND column_name = 'time' AND data_type = 'timestamp without time zone' AND table_name ~ '^userid:.{22}_export:.{22}$' OR table_name ~ '^device:.{22}_service:.{22}$';")
	if err != nil {
		return err
	}
	for rows.Next() {
		i++
		tx, err := handler.db.BeginTx(handler.ctx, &sql.TxOptions{})
		if err != nil {
			tx.Rollback()
			return err
		}
		table := ""
		err = rows.Scan(&table)
		if err != nil {
			tx.Rollback()
			return err
		}
		if handler.debug {
			log.Println("Migrating " + table)
		}
		tx, err = handler.handleColumnTypeChange(tx, table, fieldDescription{
			ColumnName: "time",
			Nullable:   false,
			DataType:   "TIMESTAMPTZ",
		}, handler.ctx, table)
		if err != nil {
			tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	if handler.debug {
		log.Printf("Finished TIMESTAMPTZ migration, ran for %v tables\n", i)
	}
	return nil
}
