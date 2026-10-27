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
	"errors"
	"strconv"
	"time"

	"github.com/SENERGY-Platform/timescale-tableworker/pkg/util"
	"github.com/lib/pq"
)

func (handler *Handler) migrate() error {
	err := handler.migrateTIMESTAMP_TIMESTAMPTZ()
	if err != nil {
		return err
	}
	return handler.migrateColumnstore()
}

func (handler *Handler) migrateTIMESTAMP_TIMESTAMPTZ() error {
	// the result set is read completely before the first transaction is opened, so that it does not
	// hold a connection for the whole migration
	tables, err := handler.queryStrings("SELECT table_name FROM information_schema.columns WHERE table_schema = 'public' AND column_name = 'time' AND data_type = 'timestamp without time zone' AND (table_name ~ '^userid:.{22}_export:.{22}$' OR table_name ~ '^device:.{22}_service:.{22}$');")
	if err != nil {
		return err
	}
	for _, table := range tables {
		tx, err := handler.db.BeginTx(handler.ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		util.Logger.Debug("Migrating " + table)
		tx, err = handler.handleColumnTypeChange(tx, table, fieldDescription{
			ColumnName: "time",
			Nullable:   false,
			DataType:   "TIMESTAMPTZ",
		}, handler.ctx, table)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
	}
	util.Logger.Debug("Finished TIMESTAMPTZ migration, ran for " + strconv.Itoa(len(tables)) + " tables")
	return nil
}

// migrateColumnstore enables the columnstore on all device tables that do not have it yet and adds the policy to device
// tables that have the columnstore but no policy (a paused policy still counts as existing). Tables that cannot be locked
// within columnstoreMigrationLockTimeout are retried, in up to columnstoreMigrationPasses passes in total. Tables that are
// still locked after the last pass are left for the next start.
func (handler *Handler) migrateColumnstore() error {
	if handler.conf.ColumnstoreAfter == "" || handler.distributed {
		return nil
	}
	// the result set is read completely before the first transaction is opened, so that it does not
	// hold a connection for the whole migration
	tables, err := handler.queryStrings("SELECT h.hypertable_name FROM timescaledb_information.hypertables h WHERE h.hypertable_schema = 'public' AND h.hypertable_name ~ '^device:.{22}_service:.{22}$' AND (NOT h.compression_enabled OR NOT EXISTS (SELECT 1 FROM timescaledb_information.jobs j WHERE j.proc_name = 'policy_compression' AND j.hypertable_schema = 'public' AND j.hypertable_name = h.hypertable_name)) ORDER BY h.hypertable_name;")
	if err != nil {
		return err
	}
	n := len(tables)
	util.Logger.Info("Columnstore migration: " + strconv.Itoa(n) + " tables to convert")
	start := time.Now()
	// slots keep the spread of the first policy runs; a retried table keeps its original slot
	slots := make(map[string]time.Time, n)
	for i, table := range tables {
		slots[table] = start.Add(time.Duration(int64(columnstoreMigrationSpread) * int64(i) / int64(n)))
	}
	converted := 0
	pending := tables
	processed := 0
	for pass := 1; pass <= columnstoreMigrationPasses && len(pending) > 0; pass++ {
		locked := []string{}
		for _, table := range pending {
			initialStart := slots[table]
			err = handler.migrateColumnstoreTable(table, initialStart)
			if err != nil {
				var pqErr *pq.Error
				if errors.As(err, &pqErr) && pqErr.Code == "55P03" {
					util.Logger.Warn("Columnstore migration: could not lock " + table + " in pass " + strconv.Itoa(pass))
					locked = append(locked, table)
				} else {
					return err
				}
			} else {
				converted++
			}
			processed++
			if processed%500 == 0 {
				util.Logger.Info("Columnstore migration: processed " + strconv.Itoa(processed) + " tables")
			}
		}
		pending = locked
	}
	for _, table := range pending {
		util.Logger.Warn("Columnstore migration: " + table + " still locked after " + strconv.Itoa(columnstoreMigrationPasses) + " passes, left for the next start")
	}
	util.Logger.Info("Finished columnstore migration, converted " + strconv.Itoa(converted) + " tables, skipped after retries " + strconv.Itoa(len(pending)))
	return nil
}

func (handler *Handler) migrateColumnstoreTable(table string, initialStart time.Time) error {
	tx, err := handler.db.BeginTx(handler.ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	_, err = tx.Exec("SET LOCAL lock_timeout = '" + columnstoreMigrationLockTimeout + "';")
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	util.Logger.Debug("Enabling columnstore for " + table)
	err = handler.enableColumnstore(tx, table, &initialStart)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
