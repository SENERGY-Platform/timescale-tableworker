/*
 * Copyright 2023 InfAI (CC SES)
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
	"github.com/SENERGY-Platform/device-repository/lib/client"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/config"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/devicetypes"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/test/docker"
	_ "github.com/lib/pq"
	"log"
	"sync"
	"testing"
	"time"
)

func TestHandler(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf, err := config.LoadConfig("../../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	conf.PostgresHost, conf.PostgresPort, conf.PostgresUser, conf.PostgresPw, conf.PostgresDb, err = docker.Timescale(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		conf.PostgresHost,
		conf.PostgresPort,
		conf.PostgresUser,
		conf.PostgresPw,
		conf.PostgresDb)

	log.Println("Connecting to PSQL...", psqlconn)
	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		t.Error(err)
		return
	}

	deviceRepoClient, deviceRepoDb, _, err := client.NewTestClient()
	if err != nil {
		t.Error(err)
		return
	}

	handler := Handler{
		db:          db,
		distributed: false,
		replication: "",
		deviceRepo:  deviceRepoClient,
		debug:       false,
		ctx:         ctx,
		conf:        conf,
		producer: &testProducer{f: func(topic string, msg string) error {
			fmt.Printf("TEST-DEBUG: produce topic=%v msg=%v\n", topic, msg)
			return nil
		}},
	}

	err = handler.initMetadataSchema()
	if err != nil {
		t.Error(err)
		return
	}

	now := time.Time{} //not really 'now', but a knowable one for testing

	testDtPut := func(t *testing.T, dt models.DeviceType, currentTime time.Time) (dtId string) {
		dt.GenerateId()
		timeout, _ := context.WithTimeout(context.Background(), 3*time.Second)
		t.Run("device-repo", func(t *testing.T) {
			err = deviceRepoDb.SetDeviceType(timeout, dt)
			if err != nil {
				t.Error(err)
				return
			}
		})
		t.Run("handler", func(t *testing.T) {
			pl, err := json.Marshal(devicetypes.DeviceTypeCommand{
				Command:    devicetypes.PutCommand,
				Id:         dt.Id,
				Owner:      "test",
				DeviceType: dt,
			})
			if err != nil {
				t.Error(err)
				return
			}
			err = handler.HandleMessage(conf.KafkaTopicDeviceTypes, pl, currentTime)
			if err != nil {
				t.Error(err)
				return
			}
		})
		return dt.Id
	}

	testDevicePut := func(t *testing.T, d models.Device, currentTime time.Time) (id string) {
		d.GenerateId()
		timeout, _ := context.WithTimeout(context.Background(), 3*time.Second)
		t.Run("device-repo", func(t *testing.T) {
			err = deviceRepoDb.SetDevice(timeout, d)
			if err != nil {
				t.Error(err)
				return
			}
		})
		t.Run("handler", func(t *testing.T) {
			pl, err := json.Marshal(deviceCommand{
				Command: devicetypes.PutCommand,
				Id:      d.Id,
				Owner:   "test",
				Device: device{
					Id:           d.Id,
					LocalId:      d.LocalId,
					Name:         d.Name,
					DeviceTypeId: d.DeviceTypeId,
				},
			})
			if err != nil {
				t.Error(err)
				return
			}
			err = handler.HandleMessage(conf.KafkaTopicDevices, pl, currentTime)
			if err != nil {
				t.Error(err)
				return
			}
		})
		return d.Id
	}

	var simpleDtId, multiPartDtId string

	t.Run("simple device-type creation", func(t *testing.T) {
		simpleDtId = testDtPut(t, models.DeviceType{
			Name:          "reduced-snowflake-canary-device-type",
			Description:   "used for canary service github.com/SENERGY-Platform/snowflake-canary",
			DeviceClassId: "urn:infai:ses:device-class:997937d6-c5f3-4486-b67c-114675038393",
			Attributes:    []models.Attribute{},
			Services: []models.Service{
				{
					LocalId:     "cmd",
					Name:        "cmd",
					Description: "canary cmd service, needed to test online state by subscription",
					Interaction: models.REQUEST,
					ProtocolId:  "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
					Inputs: []models.Content{
						{
							ContentVariable: models.ContentVariable{
								Name: "commands",
								Type: models.Structure,
								SubContentVariables: []models.ContentVariable{
									{
										Name: "valueCommand",
										Type: models.Structure,
										SubContentVariables: []models.ContentVariable{
											{
												Name:                 "value",
												Type:                 "https://schema.org/Integer",
												CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
												FunctionId:           "urn:infai:ses:controlling-function:99240d90-02dd-4d4f-a47c-069cfe77629c",
												SerializationOptions: []string{models.SerializationOptionXmlAttribute},
											},
										},
									},
								},
							},
							Serialization:     models.XML,
							ProtocolSegmentId: "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65",
						},
					},
				},
				{
					LocalId:     "sensor",
					Name:        "sensor",
					Description: "canary sensor service, needed to test device data handling",
					Interaction: models.EVENT_AND_REQUEST,
					ProtocolId:  "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
					Outputs: []models.Content{
						{
							ContentVariable: models.ContentVariable{
								Name: "measurements",
								Type: models.Structure,
								SubContentVariables: []models.ContentVariable{
									{
										Name: "measurement",
										Type: models.Structure,
										SubContentVariables: []models.ContentVariable{
											{
												Name:                 "value",
												Type:                 "https://schema.org/Integer",
												CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
												FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
												AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
												SerializationOptions: []string{models.SerializationOptionXmlAttribute},
											},
										},
									},
								},
							},
							Serialization:     models.XML,
							ProtocolSegmentId: "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65",
						},
					},
				},
			},
		}, now.Add(1*time.Second))
	})
	t.Run("multi-part device-type creation", func(t *testing.T) {
		multiPartDtId = testDtPut(t, models.DeviceType{
			Name:          "snowflake-canary-device-type",
			Description:   "used for canary service github.com/SENERGY-Platform/snowflake-canary",
			DeviceClassId: "urn:infai:ses:device-class:997937d6-c5f3-4486-b67c-114675038393",
			Attributes:    []models.Attribute{},
			Services: []models.Service{
				{
					LocalId:     "cmd",
					Name:        "cmd",
					Description: "canary cmd service, needed to test online state by subscription",
					Interaction: models.REQUEST,
					ProtocolId:  "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
					Inputs: []models.Content{
						{
							ContentVariable: models.ContentVariable{
								Name: "commands",
								Type: models.Structure,
								SubContentVariables: []models.ContentVariable{
									{
										Name: "valueCommand",
										Type: models.Structure,
										SubContentVariables: []models.ContentVariable{
											{
												Name:                 "value",
												Type:                 "https://schema.org/Integer",
												CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
												FunctionId:           "urn:infai:ses:controlling-function:99240d90-02dd-4d4f-a47c-069cfe77629c",
												SerializationOptions: []string{models.SerializationOptionXmlAttribute},
											},
										},
									},
								},
							},
							Serialization:     models.XML,
							ProtocolSegmentId: "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65",
						},
						{
							ContentVariable: models.ContentVariable{
								Name:             "flag",
								Type:             "https://schema.org/Text",
								CharacteristicId: "urn:infai:ses:characteristic:7621686a-56bc-402d-b4cc-5b266d39736f",
								FunctionId:       "urn:infai:ses:controlling-function:39b0e578-0111-4c95-994b-0d6728d474b3",
							},
							Serialization:     models.PlainText,
							ProtocolSegmentId: "urn:infai:ses:protocol-segment:9956d8b5-46fa-4381-a227-c1df69808997",
						},
					},
				},
				{
					LocalId:     "sensor",
					Name:        "sensor",
					Description: "canary sensor service, needed to test device data handling",
					Interaction: models.EVENT_AND_REQUEST,
					ProtocolId:  "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
					Outputs: []models.Content{
						{
							ContentVariable: models.ContentVariable{
								Name: "measurements",
								Type: models.Structure,
								SubContentVariables: []models.ContentVariable{
									{
										Name: "measurement",
										Type: models.Structure,
										SubContentVariables: []models.ContentVariable{
											{
												Name:                 "value",
												Type:                 "https://schema.org/Integer",
												CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
												FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
												AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
												SerializationOptions: []string{models.SerializationOptionXmlAttribute},
											},
										},
									},
								},
							},
							Serialization:     models.XML,
							ProtocolSegmentId: "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65",
						},
						{
							ContentVariable: models.ContentVariable{
								Name:             "area",
								Type:             "https://schema.org/Float",
								CharacteristicId: "urn:infai:ses:characteristic:733d95d9-f7d7-4f2e-9778-14eed5a91824",
								FunctionId:       "urn:infai:ses:measuring-function:f4f74bfc-7a58-42cb-855a-e540d566c2fc",
								AspectId:         "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
							},
							Serialization:     models.JSON,
							ProtocolSegmentId: "urn:infai:ses:protocol-segment:9956d8b5-46fa-4381-a227-c1df69808997",
						},
					},
				},
			},
		}, now)
	})

	t.Run("simple device creation", func(t *testing.T) {
		testDevicePut(t, models.Device{
			LocalId:      "simple",
			Name:         "simple",
			DeviceTypeId: simpleDtId,
		}, now.Add(2*time.Second))
	})
	t.Run("multi-part device creation", func(t *testing.T) {
		testDevicePut(t, models.Device{
			LocalId:      "multipart",
			Name:         "multipart",
			DeviceTypeId: multiPartDtId,
		}, now.Add(3*time.Second))
	})
}

type testProducer struct {
	f func(topic string, msg string) error
}

func (this *testProducer) Publish(topic string, msg string) error {
	return this.f(topic, msg)
}
