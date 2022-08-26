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
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/devicetypes"
	"log"
	"strings"
	"time"
)

type device struct {
	Id           string `json:"id"`
	LocalId      string `json:"local_id"`
	Name         string `json:"name"`
	DeviceTypeId string `json:"device_type_id"`
}

type deviceCommand struct {
	Command devicetypes.Command `json:"command"`
	Id      string              `json:"id"`
	Owner   string              `json:"owner"`
	Device  device              `json:"device"`
}

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

func (handler *Handler) createDevice(d device, t time.Time) error {
	dt, err := devicetypes.GetDeviceType(d.DeviceTypeId, handler.deviceManagerUrl)
	if err != nil {
		log.Println("Could not get device type")
		return err
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
		err = handler.createDeviceServiceTable(shortDeviceId, service)
		if err != nil {
			return err
		}
	}
	return nil
}

func (handler *Handler) createDeviceServiceTable(shortDeviceId string, service devicetypes.Service) error {
	shortServiceId, err := devicetypes.ShortenId(service.Id)
	if err != nil {
		return err
	}
	table := "device:" + shortDeviceId + "_" + "service:" + shortServiceId
	query := "CREATE TABLE IF NOT EXISTS \"" + table + "\" (time TIMESTAMP NOT NULL"
	if len(service.Outputs) > 0 {
		query += ","
	}
	for _, output := range service.Outputs {
		query += strings.Join(parseContentVariable(output.ContentVariable, ""), ",")
	}
	query += ");"
	ctx, cancel := context.WithTimeout(handler.ctx, time.Second*30)
	tx, err := handler.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		cancel()
		return err
	}
	if handler.debug {
		log.Println("Executing:", query)
	}
	_, err = tx.Exec(query)
	if err != nil {
		_ = tx.Rollback()
		cancel()
		return err
	}
	err = tx.Commit()
	if err != nil {
		cancel()
		return err
	}
	tx, err = handler.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		cancel()
		return err
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
			return err
		}
	} else {
		err = tx.Commit()
		if err != nil {
			cancel()
			return err
		}
	}
	tx, err = handler.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		cancel()
		return err
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
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		cancel()
		return err
	}
	cancel()
	return nil
}

func (handler *Handler) deleteDevice(deviceId string) error {
	shortId, err := devicetypes.ShortenId(deviceId)
	if err != nil {
		return err
	}
	err = handler.deleteTables(shortId, "%")
	if err != nil {
		return err
	}
	query := "DELETE FROM \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableDeviceTypeDevices + "\" WHERE " + fieldDeviceId + " = '" + deviceId + "';"
	if handler.debug {
		log.Println(query)
	}
	_, err = handler.db.Exec(query)
	return err
}

func (handler *Handler) deleteTables(shortDeviceId string, shortServiceId string) error {
	ctx, cancel := context.WithTimeout(handler.ctx, time.Second*30)
	tx, err := handler.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		cancel()
		return err
	}
	query := "SELECT table_name FROM information_schema.tables WHERE table_name like 'device:" + shortDeviceId + "_service:" + shortServiceId + "';"
	if handler.debug {
		log.Println("Executing:", query)
	}
	res, err := handler.db.Query(query)
	if err != nil {
		_ = tx.Rollback()
		cancel()
		return err
	}
	for res.Next() {
		var table []byte
		err = res.Scan(&table)
		if err != nil {
			_ = tx.Rollback()
			cancel()
			return err
		}

		query := "DROP TABLE \"" + string(table) + "\""
		if handler.debug {
			log.Println("Executing:", query)
		}

		_, err := tx.Exec(query)
		if err != nil {
			_ = tx.Rollback()
			cancel()
			return err
		}
	}
	err = res.Err()
	if err != nil {
		_ = tx.Rollback()
		cancel()
		return err
	}
	err = tx.Commit()
	cancel()
	return err
}
