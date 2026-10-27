/*
 * Copyright 2026 InfAI (CC SES)
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
	"strings"
	"time"

	"github.com/SENERGY-Platform/timescale-tableworker/pkg/util"
)

const (
	// columnstorePolicySchedule is how often the policy job of each table runs.
	columnstorePolicySchedule = "1 day"
	// columnstoreMigrationSpread: the first policy runs of the one-time migration are spread over this window, so that
	// compression of ~514 GiB does not run all at once and is done before the nightly backup at 00:00 UTC
	// (deploy is planned around 17:00 UTC).
	columnstoreMigrationSpread = 6 * time.Hour
	// columnstoreMigrationLockTimeout bounds the wait for the table lock of a single table during the migration.
	// While ALTER TABLE waits for ACCESS EXCLUSIVE, every new INSERT/SELECT on the table queues behind it, so the wait stays short
	// and locked tables are retried instead.
	columnstoreMigrationLockTimeout = "2s"
	// columnstoreMigrationPasses is the number of passes over tables that could not be locked.
	columnstoreMigrationPasses = 3
)

// enableColumnstore turns on the columnstore for the hypertable (if not yet enabled) and adds the policy that compresses
// chunks older than conf.ColumnstoreAfter. initialStart is the first run of the policy, nil means schedule default.
// No segmentby, because each table holds exactly one device/service combination.
func (handler *Handler) enableColumnstore(tx *sql.Tx, table string, initialStart *time.Time) error {
	if strings.ContainsAny(table, ";\"") {
		return errors.New("detect possible sql injection in table name")
	}
	var enabled bool
	err := tx.QueryRow("SELECT compression_enabled FROM timescaledb_information.hypertables WHERE hypertable_schema = 'public' AND hypertable_name = $1;", table).Scan(&enabled)
	if err != nil {
		return errors.Join(errors.New("could not read compression state of "+table), err)
	}
	if !enabled {
		query := "ALTER TABLE \"" + table + "\" SET (timescaledb.enable_columnstore = true, timescaledb.segmentby = '', timescaledb.orderby = '\"time\" DESC');"
		util.Logger.Debug(query)
		_, err = tx.Exec(query)
		if err != nil {
			return errors.Join(errors.New("could not enable columnstore for "+table), err)
		}
	}
	query := "CALL add_columnstore_policy($1::regclass, after => $2::interval, schedule_interval => INTERVAL '" + columnstorePolicySchedule + "', initial_start => $3::timestamptz, if_not_exists => true);"
	util.Logger.Debug(query)
	var start any
	if initialStart != nil {
		start = *initialStart
	}
	_, err = tx.Exec(query, "\""+table+"\"", handler.conf.ColumnstoreAfter, start)
	if err != nil {
		return errors.Join(errors.New("could not add columnstore policy for "+table), err)
	}
	return nil
}

// decompressAll decompresses all compressed chunks of the table. ALTER COLUMN TYPE is not supported otherwise.
// The columnstore policy compresses the chunks again.
func (handler *Handler) decompressAll(tx *sql.Tx, table string) error {
	if strings.ContainsAny(table, ";\"") {
		return errors.New("detect possible sql injection in table name")
	}
	rows, err := tx.Query("SELECT decompress_chunk(c, if_compressed => true) FROM show_chunks($1::regclass) c;", "\""+table+"\"")
	if err != nil {
		return errors.Join(errors.New("could not decompress chunks of "+table), err)
	}
	defer rows.Close()
	for rows.Next() {
	}
	err = rows.Err()
	if err != nil {
		return errors.Join(errors.New("could not decompress chunks of "+table), err)
	}
	return rows.Close()
}
