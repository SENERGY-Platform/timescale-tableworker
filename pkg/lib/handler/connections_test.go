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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SENERGY-Platform/device-repository/lib/client"
	structlogger "github.com/SENERGY-Platform/go-service-base/struct-logger"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/config"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/devicetypes"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/test/docker"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/util"
	_ "github.com/lib/pq"
)

// maxOpenTestConns is deliberately small. A result set that is never closed keeps its
// connection checked out forever, so a leak exhausts the pool within a few messages.
const maxOpenTestConns = 2

// TestConnectionsAreReleased covers the message paths against a pool that is too small to
// absorb a leak. A leaked result set makes the pool run dry and the next query blocks
// without an error and without a timeout, which is how the service silently stops
// processing kafka messages in production.
func TestConnectionsAreReleased(t *testing.T) {
	util.InitStructLogger("debug", structlogger.ColoredTextHandlerSelector)
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf, err := config.LoadConfig("../../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	conf.Debug = true

	conf.PostgresHost, conf.PostgresPort, conf.PostgresUser, conf.PostgresPw, conf.PostgresDb, err = docker.Timescale(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		conf.PostgresHost, conf.PostgresPort, conf.PostgresUser, conf.PostgresPw, conf.PostgresDb))
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(maxOpenTestConns)

	deviceRepoClient, deviceRepoDb, err := client.NewTestClient()
	if err != nil {
		t.Error(err)
		return
	}

	handler := Handler{
		db:          db,
		distributed: false,
		replication: "",
		deviceRepo:  deviceRepoClient,
		ctx:         ctx,
		conf:        conf,
		producer: &testProducer{f: func(topic string, msg string) error {
			return nil
		}},
	}

	err = handler.initMetadataSchema()
	if err != nil {
		t.Error(err)
		return
	}

	// step runs f with a watchdog, so an exhausted pool fails the test instead of hanging
	// until the go test timeout, and checks that every connection went back to the pool.
	step := func(t *testing.T, name string, f func() error) {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			done <- f()
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(time.Minute):
			t.Fatalf("%s: still blocked after a minute, %d of %d pool connections are in use",
				name, db.Stats().InUse, maxOpenTestConns)
		}
		if inUse := db.Stats().InUse; inUse != 0 {
			t.Fatalf("%s: %d connection(s) were not returned to the pool", name, inUse)
		}
	}

	dt := leakTestDeviceType()
	device := models.Device{
		LocalId:      "connection-leak-test",
		Name:         "connection-leak-test",
		DeviceTypeId: dt.Id,
	}
	device.GenerateId()
	createdAt := time.Unix(0, 0).UTC()

	step(t, "register device type in device repo", func() error {
		return deviceRepoDb.SetDeviceType(ctx, dt, nil)
	})
	step(t, "register device in device repo", func() error {
		return deviceRepoDb.SetDevice(ctx, client.DeviceWithConnectionState{Device: device}, nil)
	})

	step(t, "create device type", func() error {
		return handler.HandleMessage(conf.KafkaTopicDeviceTypes, deviceTypeMessage(t, dt), createdAt)
	})
	step(t, "create device", func() error {
		return handler.HandleMessage(conf.KafkaTopicDevices, devicePutMessage(t, device), createdAt)
	})

	// the service is known from here on, so getKnownServiceMeta finds a row. That is the
	// path that used to leak one connection per call.
	for i := 0; i < maxOpenTestConns+3; i++ {
		step(t, fmt.Sprintf("repeat unchanged device type %v", i), func() error {
			return handler.HandleMessage(conf.KafkaTopicDeviceTypes, deviceTypeMessage(t, dt), createdAt)
		})
	}

	changed := leakTestDeviceType()
	changed.Services[0].Outputs[0].ContentVariable.SubContentVariables = append(
		changed.Services[0].Outputs[0].ContentVariable.SubContentVariables,
		models.ContentVariable{
			Name:             "value2",
			Type:             models.Integer,
			CharacteristicId: "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
			FunctionId:       "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
			AspectId:         "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
		})
	step(t, "update device type", func() error {
		return handler.HandleMessage(conf.KafkaTopicDeviceTypes, deviceTypeMessage(t, changed), createdAt.Add(time.Second))
	})

	step(t, "delete device", func() error {
		pl, err := json.Marshal(deviceCommand{
			Command: devicetypes.DeleteCommand,
			Id:      device.Id,
			Owner:   "test",
		})
		if err != nil {
			return err
		}
		return handler.HandleMessage(conf.KafkaTopicDevices, pl, createdAt.Add(2*time.Second))
	})
}

func deviceTypeMessage(t *testing.T, dt models.DeviceType) []byte {
	t.Helper()
	pl, err := json.Marshal(devicetypes.DeviceTypeCommand{
		Command:    devicetypes.PutCommand,
		Id:         dt.Id,
		Owner:      "test",
		DeviceType: dt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return pl
}

func devicePutMessage(t *testing.T, d models.Device) []byte {
	t.Helper()
	pl, err := json.Marshal(deviceCommand{
		Command: devicetypes.PutCommand,
		Id:      d.Id,
		Owner:   "test",
		Device:  d,
	})
	if err != nil {
		t.Fatal(err)
	}
	return pl
}

func leakTestDeviceType() models.DeviceType {
	return models.DeviceType{
		Name:          "connection-leak-test-device-type",
		Id:            "urn:infai:ses:device-type:3f7b1c62-6a4d-4c1e-9a2b-0c5d8e7f1a34",
		DeviceClassId: "urn:infai:ses:device-class:997937d6-c5f3-4486-b67c-114675038393",
		Attributes:    []models.Attribute{},
		Services: []models.Service{
			{
				LocalId:     "sensor",
				Name:        "sensor",
				Id:          "urn:infai:ses:service:9d1e4a70-25b8-4f6c-8e13-7a0b2c6d5e91",
				Interaction: models.EVENT_AND_REQUEST,
				ProtocolId:  "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
				Outputs: []models.Content{
					{
						ContentVariable: models.ContentVariable{
							Name: "measurement",
							Type: models.Structure,
							SubContentVariables: []models.ContentVariable{
								{
									Name:             "value",
									Type:             models.Integer,
									CharacteristicId: "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
									FunctionId:       "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
									AspectId:         "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
								},
							},
						},
						Serialization:     models.JSON,
						ProtocolSegmentId: "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65",
					},
				},
			},
		},
	}
}
