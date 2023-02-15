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
	"encoding/json"
	"errors"
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
		oldHash, lastUpdate, err := handler.getKnownServiceMeta(service.Id)
		if err != nil {
			return err
		}
		if !lastUpdate.Before(t) {
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
			return err
		}
		if handler.debug {
			log.Printf("Found %v outdated devices that need to be updated\n", len(outdatedDeviceIds))
		}
		shortServiceId, err := devicetypes.ShortenId(service.Id)
		deleted := []string{}
		created := []string{}
		for _, outdatedDeviceId := range outdatedDeviceIds {
			shortDeviceId, err := devicetypes.ShortenId(outdatedDeviceId)
			if err != nil {
				return err
			}
			tables, err := handler.deleteTables(shortDeviceId, shortServiceId)
			if err != nil {
				return err
			}
			deleted = append(deleted, tables...)
			table, err := handler.createDeviceServiceTable(shortDeviceId, service)
			if err != nil {
				return err
			}
			created = append(created, table)
		}
		err = handler.upsertServiceMeta(service.Id, newHash, t)
		if err != nil {
			return err
		}
		b, err := json.Marshal(TableEditMessage{
			Method: "delete",
			Tables: deleted,
		})
		err = handler.producer.Publish(handler.conf.KafkaTopicTableUpdates, string(b))
		b, err = json.Marshal(TableEditMessage{
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
