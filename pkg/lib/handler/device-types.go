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
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/devicetypes"
	"log"
	"time"
)

func (handler *Handler) handleDeviceTypeMessage(msg []byte, t time.Time) error {
	var cmd devicetypes.DeviceTypeCommand
	err := json.Unmarshal(msg, &cmd)
	if err != nil {
		return err
	}
	switch cmd.Command {
	case devicetypes.PutCommand:
		return handler.handleDeviceTypeUpdate(cmd.DeviceType, t)
	case devicetypes.DeleteCommand:
		return nil // device types can only be deleted if all devices are deleted first
	case devicetypes.RightsCommand:
		return nil
	default:
		return errors.New("unknown command (ignored): " + string(cmd.Command))
	}
}

func (handler *Handler) handleDeviceTypeUpdate(dt devicetypes.DeviceType, t time.Time) error {
	for _, service := range dt.Services {
		baseError := errors.New("Device Type Update " + dt.Name + " (" + dt.Id + "), Service " + service.Name + " (" + service.Id + ")")
		oldHash, lastUpdate, err := handler.getKnownServiceMeta(service.Id)
		if err != nil {
			return errors.Join(baseError, err)
		}
		if lastUpdate.After(t) {
			if handler.debug {
				log.Println("Already processed newer version, skipping update...")
			}
			continue
		}
		newHash := hashServiceOutputs(service)
		if handler.debug {
			log.Println("Old/New Hash", oldHash, newHash)
		}
		if oldHash == newHash {
			if handler.debug {
				log.Println("No relevant changes, skipping update...")
			}
			continue
		}
		outdatedDeviceIds, err := handler.getOutdatedDeviceIds(dt.Id, t)
		if err != nil {
			return errors.Join(baseError, err)
		}
		if handler.debug {
			log.Printf("Found %v outdated devices that need to be updated\n", len(outdatedDeviceIds))
		}
		shortServiceId, err := devicetypes.ShortenId(service.Id)
		created := []string{}
		for _, outdatedDeviceId := range outdatedDeviceIds {
			shortDeviceId, err := devicetypes.ShortenId(outdatedDeviceId)
			if err != nil {
				return errors.Join(baseError, err)
			}
			table := "device:" + shortDeviceId + "_service:" + shortServiceId
			currentFd, err := handler.getFieldDescriptionsOfTable(table)
			if err != nil {
				return errors.Join(baseError, err)
			}
			newFd := getFieldDescriptions(service)
			added, removed, newType, setNotNull, dropNotNull := compareFds(currentFd, newFd)
			ctx, cancel := context.WithTimeout(handler.ctx, 30*time.Second)
			defer cancel() // cancel is also called at the end of the loop, deferring it in case of an early return
			tx, err := handler.db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				return errors.Join(baseError, err)
			}
			for _, add := range added {
				_, err = tx.Exec(fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN %s;", table, add.String()))
				if err != nil {
					_ = tx.Rollback()
					return errors.Join(baseError, err)
				}
			}
			for _, rm := range removed {
				_, err = tx.Exec(fmt.Sprintf("ALTER TABLE \"%s\" DROP COLUMN %s;", table, rm.ColumnName))
				if err != nil {
					_ = tx.Rollback()
					return errors.Join(baseError, err)
				}
			}
			for _, nt := range newType {
				query := fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN %s TYPE %s;", table, nt.ColumnName, nt.DataType)
				_, err = tx.Exec(query)
				if err != nil {
					_ = tx.Rollback()
					return errors.Join(baseError, err)
				}
			}
			for _, nn := range setNotNull {
				query := fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN %s SET NOT NULL;", table, nn.ColumnName)
				_, err = tx.Exec(query)
				if err != nil {
					_ = tx.Rollback()
					return errors.Join(baseError, err)
				}
			}
			for _, nn := range dropNotNull {
				query := fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN %s DROP NOT NULL;", table, nn.ColumnName)
				_, err = tx.Exec(query)
				if err != nil {
					_ = tx.Rollback()
					return errors.Join(baseError, err)
				}
			}
			err = tx.Commit()
			if err != nil {
				_ = tx.Rollback()
				return errors.Join(baseError, err)
			}
			created = append(created, table)
			cancel()
		}
		err = handler.upsertServiceMeta(service.Id, newHash, t)
		if err != nil {
			return err
		}
		b, err := json.Marshal(TableEditMessage{
			Method: "put",
			Tables: created,
		})
		err = handler.producer.Publish(handler.conf.KafkaTopicTableUpdates, string(b))
	}
	return nil
}

func (handler *Handler) getKnownServiceMeta(serviceId string) (hash string, t time.Time, err error) {
	query := "SELECT \"" + fieldHash + "\", " + "\"" + fieldTime + "\" FROM \"" + handler.conf.PostgresTableworkerSchema +
		"\".\"" + tableServiceHashes + "\" WHERE \"" + fieldServiceId + "\" = '" + serviceId + "';"
	if handler.debug {
		log.Println(query)
	}
	res, err := handler.db.Query(query)
	if err != nil {
		return
	}
	if res.Next() {
		err = res.Scan(&hash, &t)
	}
	return
}

func (handler *Handler) upsertServiceMeta(serviceId string, hash string, t time.Time) (err error) {
	query := "INSERT INTO \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableServiceHashes + "\" (\"" +
		fieldServiceId + "\", \"" + fieldHash + "\", \"" + fieldTime + "\") " +
		"VALUES ('" + serviceId + "', '" + hash + "', '" + t.Format(time.RFC3339Nano) + "')" +
		"ON CONFLICT ON CONSTRAINT \"" + tableServiceHashes + "_pkey\" DO UPDATE SET \"" + fieldHash + "\" = '" + hash + "', \"" + fieldTime + "\" = '" + t.Format(time.RFC3339Nano) +
		"' WHERE \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableServiceHashes + "\".\"" + fieldServiceId + "\" = '" + serviceId + "';"
	if handler.debug {
		log.Println(query)
	}
	res, err := handler.db.Query(query)
	if err != nil {
		return
	}
	if res.Next() {
		err = res.Scan(&hash, &t)
	}
	return
}

func (handler *Handler) getOutdatedDeviceIds(deviceTypeId string, t time.Time) (deviceIds []string, err error) {
	query := "SELECT \"" + fieldDeviceId + "\" FROM \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableDeviceTypeDevices + "\" WHERE \"" +
		fieldDeviceTypeId + "\" = '" + deviceTypeId + "' AND \"" + fieldTime + "\" < '" + t.Format(time.RFC3339Nano) + "';"
	if handler.debug {
		log.Println(query)
	}
	res, err := handler.db.Query(query)
	if err != nil {
		return
	}
	var deviceId string
	for res.Next() {
		err = res.Scan(&deviceId)
		if err != nil {
			return nil, err
		}
		deviceIds = append(deviceIds, deviceId)
	}
	return
}
