/*
 * Copyright 2025 InfAI (CC SES)
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
	"slices"
	"time"

	"github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/models/go/models"
)

func (handler *Handler) FullSync() error {
	var limit int64 = 1000
	var devices []models.Device
	var err error
	deviceIds := []string{}

	// ensure all devices exist in db
	for devices == nil || len(devices) == int(limit) {
		devices, err, _ = handler.deviceRepo.ListDevices(token, model.DeviceListOptions{
			Limit:  limit,
			Offset: int64(len(deviceIds)),
		})
		if err != nil {
			return err
		}
		for _, device := range devices {
			deviceIds = append(deviceIds, device.Id)
			err = handler.createDevice(device, time.Now())
			if err != nil {
				return err
			}
		}
	}

	slices.Sort(deviceIds) // enables binary search

	// ensure deleted devices are deleted in db as well
	rows, err := handler.db.Query("SELECT DISTINCT device_id FROM \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableDeviceTypeDevices + "\"")
	if err != nil {
		return err
	}
	var deviceId string
	for rows.Next() {
		err = rows.Scan(&deviceId)
		if err != nil {
			return err
		}
		_, ok := slices.BinarySearch(deviceIds, deviceId)
		if !ok {
			err = handler.deleteDevice(deviceId)
			if err != nil {
				return err
			}
		}
	}

	// ensure all device tables are up to date
	var offset int64 = 0
	var deviceTypes []models.DeviceType
	for deviceTypes == nil || len(devices) == int(limit) {
		deviceTypes, err, _ = handler.deviceRepo.ListDeviceTypesV2(token, limit, offset, "name.asc", []model.FilterCriteria{}, false, true)
		if err != nil {
			return err
		}
		offset += int64(len(deviceTypes))
		for _, deviceType := range deviceTypes {
			err = handler.handleDeviceTypeUpdate(deviceType, time.Now())
			if err != nil {
				return err
			}
		}
	}

	return nil
}
