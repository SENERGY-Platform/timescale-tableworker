/*
 * Copyright 2021 InfAI (CC SES)
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
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/devicetypes"
)

type deviceCommand struct {
	Command devicetypes.Command `json:"command"`
	Id      string              `json:"id"`
	Owner   string              `json:"owner"`
	Device  models.Device       `json:"device"`
}

// token is expired and invalid. but because this service does not validate the received tokens,
// it may be used by trusted internal services which are within the same network (kubernetes cluster).
// requests with this token may not be routed over an ingres with token validation
const token = `Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjEwMDAwMDAwMDAsImlhdCI6MTAwMDAwMDAwMCwiYXV0aF90aW1lIjoxMDAwMDAwMDAwLCJpc3MiOiJpbnRlcm5hbCIsImF1ZCI6W10sInN1YiI6ImRkNjllYTBkLWY1NTMtNDMzNi04MGYzLTdmNDU2N2Y4NWM3YiIsInR5cCI6IkJlYXJlciIsImF6cCI6ImZyb250ZW5kIiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImFkbWluIiwiZGV2ZWxvcGVyIiwidXNlciJdfSwicmVzb3VyY2VfYWNjZXNzIjp7Im1hc3Rlci1yZWFsbSI6eyJyb2xlcyI6W119LCJCYWNrZW5kLXJlYWxtIjp7InJvbGVzIjpbXX0sImFjY291bnQiOnsicm9sZXMiOltdfX0sInJvbGVzIjpbImFkbWluIiwiZGV2ZWxvcGVyIiwidXNlciJdLCJuYW1lIjoiU2VwbCBBZG1pbiIsInByZWZlcnJlZF91c2VybmFtZSI6InNlcGwiLCJnaXZlbl9uYW1lIjoiU2VwbCIsImxvY2FsZSI6ImVuIiwiZmFtaWx5X25hbWUiOiJBZG1pbiIsImVtYWlsIjoic2VwbEBzZXBsLmRlIn0.HZyG6n-BfpnaPAmcDoSEh0SadxUx-w4sEt2RVlQ9e5I`

func (handler *Handler) handleDeviceMessage(msg []byte, t time.Time) error {
	var cmd deviceCommand
	err := json.Unmarshal(msg, &cmd)
	if err != nil {
		return err
	}
	switch cmd.Command {
	case devicetypes.PutCommand:
		return handler.createDevice(cmd.Device, t)
	case devicetypes.DeleteCommand:
		return handler.deleteDevice(cmd.Id)
	case devicetypes.RightsCommand:
		return nil
	default:
		return errors.New("unknown command (ignored): " + string(cmd.Command))
	}
}

func (handler *Handler) createDevice(d models.Device, t time.Time) error {
	dt, err, _ := handler.deviceRepo.ReadDeviceType(d.DeviceTypeId, token)
	if err != nil {
		log.Println("Could not get device type", err)
		return err
	}
	editMessage := TableEditMessage{
		Method: "put",
		Tables: []string{},
	}
	query := "INSERT INTO \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableDeviceTypeDevices +
		"\" (\"" + fieldDeviceTypeId + "\", \"" + fieldDeviceId + "\", \"" + fieldTime + "\") VALUES('" + d.DeviceTypeId +
		"', '" + d.Id + "', '" + t.Format(time.RFC3339Nano) + "') ON CONFLICT DO NOTHING;"
	if handler.debug {
		log.Println(query)
	}
	_, err = handler.db.Exec(query)
	if err != nil {
		return err
	}

	shortDeviceId, err := devicetypes.ShortenId(d.Id)
	if err != nil {
		return err
	}
	for _, service := range dt.Services {
		table, err := handler.createDeviceServiceTable(shortDeviceId, service)
		if err != nil {
			return err
		}
		editMessage.Tables = append(editMessage.Tables, table)
	}
	b, err := json.Marshal(editMessage)
	if err != nil {
		return err
	}
	err = handler.producer.Publish(handler.conf.KafkaTopicTableUpdates, string(b))
	if err != nil {
		return err
	}
	return nil
}

func (handler *Handler) createDeviceServiceTable(shortDeviceId string, service devicetypes.Service) (table string, err error) {
	shortServiceId, err := devicetypes.ShortenId(service.Id)
	if err != nil {
		return table, err
	}
	table = "device:" + shortDeviceId + "_" + "service:" + shortServiceId

	fieldDescriptions := getFieldDescriptions(service)
	fieldDescriptionStrings := []string{}
	for _, fd := range fieldDescriptions {
		fieldDescriptionStrings = append(fieldDescriptionStrings, fd.String())
	}

	fieldDescriptionString := strings.Join(fieldDescriptionStrings, ",")

	if strings.ContainsAny(table, ";") {
		return table, errors.New("detect possible sql injection in table name")
	}
	if strings.ContainsAny(fieldDescriptionString, ";") {
		return table, errors.New("detect possible sql injection in content-variable name")
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%v" ( %v );`, table, fieldDescriptionString)
	ctx, cancel := context.WithTimeout(handler.ctx, time.Second*120)
	tx, err := handler.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		cancel()
		return table, err
	}
	if handler.debug {
		log.Println("Executing:", query)
	}
	_, err = tx.Exec(query)
	if err != nil {
		_ = tx.Rollback()
		cancel()
		return table, err
	}
	err = tx.Commit()
	if err != nil {
		cancel()
		return table, err
	}
	tx, err = handler.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		cancel()
		return table, err
	}
	query = "SELECT create_"
	if handler.distributed {
		query += "distributed_"
	}
	query += "hypertable('\"" + table + "\"','time');"
	if handler.debug {
		log.Println("Executing:", query)
	}
	_, err = tx.Exec(query)
	if err != nil {
		_ = tx.Rollback()
		if err.Error() == "pq: table \""+table+"\" is already a hypertable" {
			log.Println("INFO: " + err.Error())
		} else {
			cancel()
			return table, err
		}
	} else {
		err = tx.Commit()
		if err != nil {
			cancel()
			return table, err
		}
	}
	tx, err = handler.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		cancel()
		return table, err
	}
	if handler.distributed {
		query = "SELECT set_replication_factor('\"" + table + "\"', " + handler.replication + ");"
		if handler.debug {
			log.Println("Executing:", query)
		}
		_, err = tx.Exec(query)
		if err != nil {
			_ = tx.Rollback()
			cancel()
			return table, err
		}
	}
	err = tx.Commit()
	if err != nil {
		cancel()
		return table, err
	}
	cancel()
	return table, err
}

func (handler *Handler) deleteDevice(deviceId string) error {
	shortId, err := devicetypes.ShortenId(deviceId)
	if err != nil {
		return err
	}
	tables, err := handler.deleteTables(shortId, "%")
	if err != nil {
		return err
	}
	query := "DELETE FROM \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableDeviceTypeDevices + "\" WHERE " + fieldDeviceId + " = '" + deviceId + "';"
	if handler.debug {
		log.Println(query)
	}
	_, err = handler.db.Exec(query)
	if err != nil {
		return err
	}
	b, err := json.Marshal(TableEditMessage{
		Method: "delete",
		Tables: tables,
	})
	if err != nil {
		return err
	}
	err = handler.producer.Publish(handler.conf.KafkaTopicTableUpdates, string(b))
	return err
}

func (handler *Handler) deleteTables(shortDeviceId string, shortServiceId string) (tables []string, err error) {
	ctx, cancel := context.WithTimeout(handler.ctx, time.Second*120)
	tx, err := handler.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		cancel()
		return tables, err
	}
	query := "SELECT table_name FROM information_schema.tables WHERE table_name like 'device:" + shortDeviceId + "_service:" + shortServiceId + "';"
	if handler.debug {
		log.Println("Executing:", query)
	}
	res, err := handler.db.Query(query)
	if err != nil {
		_ = tx.Rollback()
		cancel()
		return tables, err
	}
	tables = []string{}
	for res.Next() {
		var table []byte
		err = res.Scan(&table)
		if err != nil {
			_ = tx.Rollback()
			cancel()
			return tables, err
		}
		tables = append(tables, string(table))

		query := "DROP TABLE IF EXISTS \"" + string(table) + "\" CASCADE"
		if handler.debug {
			log.Println("Executing:", query)
		}

		_, err := tx.Exec(query)
		if err != nil {
			_ = tx.Rollback()
			cancel()
			return tables, err
		}
	}
	err = res.Err()
	if err != nil {
		_ = tx.Rollback()
		cancel()
		return tables, err
	}
	err = tx.Commit()
	cancel()
	return tables, err
}

func getFieldDescriptionsOfTable(table string, tx *sql.Tx) ([]fieldDescription, error) {
	res := []fieldDescription{}
	rows, err := tx.Query(fmt.Sprintf("SELECT column_name, is_nullable, data_type from information_schema.columns where table_name = '%s'", table))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		fd := fieldDescription{}
		no := ""
		err = rows.Scan(&fd.ColumnName, &no, &fd.DataType)
		if err != nil {
			return nil, err
		}
		fd.ColumnName = "\"" + fd.ColumnName + "\""
		if strings.ToUpper(no) != "NO" {
			fd.Nullable = true
		}
		res = append(res, fd)
	}
	return res, nil
}

func (handler *Handler) tableExists(table string) (found bool, err error) {
	row := handler.db.QueryRow(fmt.Sprintf("SELECT count(table_name) > 0 FROM information_schema.tables WHERE table_name = '%s'", table))
	err = row.Scan(&found)
	return
}

func getFieldDescriptions(service devicetypes.Service) []fieldDescription {
	res := []fieldDescription{{
		ColumnName: "\"time\"",
		Nullable:   false,
		DataType:   "timestamp with time zone",
	}}
	for _, output := range service.Outputs {
		res = append(res, parseContentVariable(output.ContentVariable, "")...)
	}
	return res
}
