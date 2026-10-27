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
	"slices"
	"strings"
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
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestHandler(t *testing.T) {
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

	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		conf.PostgresHost,
		conf.PostgresPort,
		conf.PostgresUser,
		conf.PostgresPw,
		conf.PostgresDb)

	util.Logger.Info("Connecting to PSQL... " + psqlconn)
	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		t.Error(err)
		return
	}

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
			fmt.Printf("TEST-DEBUG: produce topic=%v msg=%v\n", topic, msg)
			return nil
		}},
	}

	err = handler.initMetadataSchema()
	if err != nil {
		t.Error(err)
		return
	}

	now := time.Unix(0, 0).UTC() //not really 'now', but a knowable one for testing

	testDtPut := func(t *testing.T, dt models.DeviceType, currentTime time.Time) models.DeviceType {
		timeout, _ := context.WithTimeout(context.Background(), 3*time.Second)
		t.Run("device-repo", func(t *testing.T) {
			err = deviceRepoDb.SetDeviceType(timeout, dt, nil)
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
		return dt
	}

	testDevicePut := func(t *testing.T, d models.Device, currentTime time.Time) (id string) {
		d.GenerateId()
		timeout, _ := context.WithTimeout(context.Background(), 3*time.Second)
		t.Run("device-repo", func(t *testing.T) {
			err = deviceRepoDb.SetDevice(timeout, client.DeviceWithConnectionState{Device: d}, nil)
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
				Device: models.Device{
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

	simpleDt := models.DeviceType{
		Name:          "reduced-snowflake-canary-device-type",
		Id:            "urn:infai:ses:device-type:2d9c189a-e7d9-4576-bdb9-a1f7b07e5eb6",
		Description:   "used for canary service github.com/SENERGY-Platform/snowflake-canary",
		DeviceClassId: "urn:infai:ses:device-class:997937d6-c5f3-4486-b67c-114675038393",
		Attributes:    []models.Attribute{},
		Services: []models.Service{
			{
				LocalId:     "cmd",
				Name:        "cmd",
				Id:          "urn:infai:ses:service:f08f5ea6-f45e-45ee-852e-7ac6664ba920",
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
											Type:                 models.Integer,
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
				Id:          "urn:infai:ses:service:bdd2a9e2-32f4-4809-b779-67af13d4bf13",
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
											Type:                 models.Integer,
											CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
											FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
											AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
											SerializationOptions: []string{models.SerializationOptionXmlAttribute},
										},
										{
											Name:                 "value2",
											Type:                 models.Integer,
											CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
											FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
											AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
											SerializationOptions: []string{models.SerializationOptionXmlAttribute},
										},
										{
											Name:                 "thisisatestforveryveryveryveryveryverylongfieldnameswhichneedtobehashed",
											Type:                 models.Integer,
											CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
											FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
											AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
											SerializationOptions: []string{models.SerializationOptionXmlAttribute},
										},
									},
								},
								{
									Name: "listvariable",
									Type: models.List,
									SubContentVariables: []models.ContentVariable{
										{
											Name: "*",
											Type: models.Structure,
											SubContentVariables: []models.ContentVariable{
												{
													Name:                 "value",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
												{
													Name:                 "value2",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
											},
										},
									},
								},
								{
									Name: "listfixed",
									Type: models.List,
									SubContentVariables: []models.ContentVariable{
										{
											Name: "0",
											Type: models.Structure,
											SubContentVariables: []models.ContentVariable{
												{
													Name:                 "value",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
												{
													Name:                 "value2",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
											},
										},
										{
											Name: "1",
											Type: models.Structure,
											SubContentVariables: []models.ContentVariable{
												{
													Name:                 "value",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
												{
													Name:                 "value2",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
											},
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
	}

	var multiPartDt models.DeviceType

	t.Run("simple device-type creation", func(t *testing.T) {
		simpleDt = testDtPut(t, simpleDt, now.Add(1*time.Second))
		if simpleDt.Services[1].Outputs[0].ContentVariable.SubContentVariables[0].SubContentVariables[0].Name != "value" {
			t.Error("wrong content variable name") // sanity check, never stopped here though...
		}
	})
	t.Run("multi-part device-type creation", func(t *testing.T) {
		multiPartDt = testDtPut(t, models.DeviceType{
			Name:          "snowflake-canary-device-type",
			Id:            "urn:infai:ses:device-type:902760cc-7efd-47b6-acb3-2170e7fb780c",
			Description:   "used for canary service github.com/SENERGY-Platform/snowflake-canary",
			DeviceClassId: "urn:infai:ses:device-class:997937d6-c5f3-4486-b67c-114675038393",
			Attributes:    []models.Attribute{},
			Services: []models.Service{
				{
					LocalId:     "cmd",
					Name:        "cmd",
					Id:          "urn:infai:ses:service:f08f5ea6-f45e-45ee-852e-7ac6664ba920",
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
												Type:                 models.Integer,
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
					Id:          "urn:infai:ses:service:bdd2a9e2-32f4-4809-b779-67af13d4bf13",
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
												Type:                 models.Integer,
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

	t.Run("simple device creation, device type update", func(t *testing.T) {
		deviceId := testDevicePut(t, models.Device{
			LocalId:      "simple",
			Name:         "simple",
			DeviceTypeId: simpleDt.Id,
		}, now.Add(2*time.Second))
		shortDeviceId, err := models.ShortenId(deviceId)
		if err != nil {
			t.Error(err)
			return
		}
		shortServiceId, err := models.ShortenId(simpleDt.Services[1].Id)
		if err != nil {
			t.Error(err)
			return
		}
		table := "device:" + shortDeviceId + "_service:" + shortServiceId
		validate := func(t *testing.T) {
			row := handler.db.QueryRow("SELECT count(*) FROM \"" + table + "\";")
			var count int64
			err = row.Scan(&count)
			if err != nil {
				t.Error(err)
				return
			}
			if count != int64(1) {
				t.Error("Deleted data")
			}
			row = handler.db.QueryRow("SELECT count(*) FROM \"" + table + "_ld\";")
			err = row.Scan(&count)
			if err != nil {
				t.Error(err)
				return
			}
			if count != int64(1) {
				t.Error("Deleted data in view")
			}
			var val string
			err = handler.db.QueryRow("SELECT \"measurements.measurement.value\" FROM \"" + table + "_ld\" ORDER BY time DESC LIMIT 1;").Scan(&val)
			if err != nil {
				t.Error(err)
				return
			}
			if val != "0" {
				t.Error("Wrong data in view")
			}
			row = handler.db.QueryRow("SELECT count(*) FROM \"" + table + "_fd\";")
			err = row.Scan(&count)
			if err != nil {
				t.Error(err)
				return
			}
			if count != int64(1) {
				t.Error("DeviceType Update deleted data in view")
			}
			err = handler.db.QueryRow("SELECT \"measurements.measurement.value\" FROM \"" + table + "_fd\" ORDER BY time DESC LIMIT 1;").Scan(&val)
			if err != nil {
				t.Error(err)
				return
			}
			if val != "0" {
				t.Error("DeviceType Update wrong data in view")
			}
			ctx, cancel = context.WithTimeout(handler.ctx, 10*time.Minute)
			defer cancel()
			tx, err := handler.db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			fd, err := getFieldDescriptionsOfTable(table+"_ld", tx)
			if err != nil {
				t.Error(err)
				return
			}
			slices.SortFunc(fd, func(a, b fieldDescription) int {
				return strings.Compare(a.ColumnName, b.ColumnName)
			})
			if len(fd) != 8 || fd[1].DataType != "bigint" || fd[6].ColumnName != "\"measurements.measurement.value3\"" || fd[5].DataType != "text" || fd[5].ColumnName != "\"measurements.measurement.value\"" {
				t.Errorf("Expected different field descriptions:\n%#v\n", fd)
				return
			}
			fd, err = getFieldDescriptionsOfTable(table+"_fd", tx)
			if err != nil {
				t.Error(err)
				return
			}
			err = tx.Rollback()
			if err != nil {
				t.Error(err)
				return
			}
			cancel()
			slices.SortFunc(fd, func(a, b fieldDescription) int {
				return strings.Compare(a.ColumnName, b.ColumnName)
			})
			if len(fd) != 8 || fd[6].DataType != "bigint" || fd[6].ColumnName != "\"measurements.measurement.value3\"" || fd[5].DataType != "text" || fd[5].ColumnName != "\"measurements.measurement.value\"" {
				t.Errorf("Expected different field descriptions:\n%#v\n", fd)
				return
			}
		}
		t.Run("device type update", func(t *testing.T) {

			_, err = handler.db.Exec("INSERT INTO \"" + table + "\"(\"measurements.measurement.value\", \"measurements.measurement.value2\", \"measurements.listvariable\", \"measurements.listfixed.0.value\", \"measurements.listfixed.0.value2\", \"measurements.listfixed.1.value\", \"measurements.listfixed.1.value2\", time) VALUES (0, 1, '[{\"value\": 12, \"value2\": 34}, {\"value\": 56, \"value2\": 78}, {\"value\": 90, \"value2\": 12}]', 12, 34, 56, 78, '1970-01-01T00:00:00');") // this tests correct column creation
			if err != nil {
				t.Error(err)
				return
			}
			_, err = handler.db.Exec("CREATE MATERIALIZED VIEW IF NOT EXISTS \"" + table + "_ld\"" +
				"WITH (timescaledb.continuous) AS " +
				"SELECT " +
				"time_bucket(INTERVAL '1 day', time) AS time, " +
				"last(\"measurements.measurement.value\", time) AS \"measurements.measurement.value\", " +
				"last(\"measurements.measurement.value2\", time) AS \"measurements.measurement.value2\", " +
				"last(\"measurements.listfixed.0.value\", time) AS \"measurements.listfixed.0.value\", " +
				"last(\"measurements.listfixed.0.value2\", time) AS \"measurements.listfixed.0.value2\", " +
				"last(\"measurements.listfixed.1.value\", time) AS \"measurements.listfixed.1.value\", " +
				"last(\"measurements.listfixed.1.value2\", time) AS \"measurements.listfixed.1.value2\", " +
				"last(\"measurements.listvariable\", time) AS \"measurements.listvariable\" " +
				"FROM \"" + table + "\" " +
				"GROUP BY  time_bucket(INTERVAL '1 day', time) " +
				"WITH NO DATA;")
			if err != nil {
				t.Error(err)
				return
			}
			_, err = handler.db.Exec("CALL refresh_continuous_aggregate('\"" + table + "_ld\"', NULL, NULL);")
			if err != nil {
				t.Error(err)
				return
			}
			_, err = handler.db.Exec("CREATE MATERIALIZED VIEW IF NOT EXISTS \"" + table + "_fd\"" +
				"WITH (timescaledb.continuous) AS " +
				"SELECT " +
				"time_bucket(INTERVAL '1 day', time) AS time, " +
				"first(\"measurements.measurement.value\", time) AS \"measurements.measurement.value\", " +
				"first(\"measurements.measurement.value2\", time) AS \"measurements.measurement.value2\", " +
				"first(\"measurements.listfixed.0.value\", time) AS \"measurements.listfixed.0.value\", " +
				"first(\"measurements.listfixed.0.value2\", time) AS \"measurements.listfixed.0.value2\", " +
				"first(\"measurements.listfixed.1.value\", time) AS \"measurements.listfixed.1.value\", " +
				"first(\"measurements.listfixed.1.value2\", time) AS \"measurements.listfixed.1.value2\", " +
				"first(\"measurements.listvariable\", time) AS \"measurements.listvariable\" " +
				"FROM \"" + table + "\" " +
				"GROUP BY  time_bucket(INTERVAL '1 day', time) " +
				"WITH NO DATA;")
			if err != nil {
				t.Error(err)
				return
			}
			_, err = handler.db.Exec("CALL refresh_continuous_aggregate('\"" + table + "_fd\"', NULL, NULL);")
			if err != nil {
				t.Error(err)
				return
			}
			ctx, cancel := context.WithTimeout(handler.ctx, 10*time.Minute)
			defer cancel()
			tx, err := handler.db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			i := 0
			err = handler.forEachCAofHypertable(table, tx, func(table string, viewSchema string, viewName string, viewDefinition string, materialized_only bool) error {
				i++
				return nil
			})
			if err != nil {
				t.Error(err)
				return
			}
			err = tx.Rollback()
			if err != nil {
				t.Error(err)
				return
			}
			cancel()
			if i != 2 {
				t.Fatal("wrong number of hypertables")
			}
			simpleDt.Services[1].Outputs[0].ContentVariable.SubContentVariables[0].SubContentVariables[0].Type = models.String
			simpleDt.Services[1].Outputs[0].ContentVariable.SubContentVariables[0].SubContentVariables = simpleDt.Services[1].Outputs[0].ContentVariable.SubContentVariables[0].SubContentVariables[0:1]
			newCv := models.ContentVariable{
				Name:                 "value3",
				Type:                 models.Integer,
				CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
				FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
				AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
				SerializationOptions: []string{models.SerializationOptionXmlAttribute},
			}
			newCv.GenerateId()
			simpleDt.Services[1].Outputs[0].ContentVariable.SubContentVariables[0].SubContentVariables = append(simpleDt.Services[1].Outputs[0].ContentVariable.SubContentVariables[0].SubContentVariables, newCv)
			pl, err := json.Marshal(devicetypes.DeviceTypeCommand{
				Command:    devicetypes.PutCommand,
				Id:         simpleDt.Id,
				Owner:      "test",
				DeviceType: simpleDt,
			})
			if err != nil {
				t.Error(err)
				return
			}
			err = handler.HandleMessage(conf.KafkaTopicDeviceTypes, pl, now.Add(24*time.Hour))
			if err != nil {
				t.Error(err)
				return
			}
			validate(t)

		})
		t.Run("migrateTZ", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(handler.ctx, 10*time.Minute)
			defer cancel()
			tx, err := handler.db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			tx, err = handler.handleColumnTypeChange(tx, table, fieldDescription{
				ColumnName: "time",
				Nullable:   false,
				DataType:   "TIMESTAMP",
			}, handler.ctx, table)
			if err != nil {
				t.Error(err)
				return
			}
			err = tx.Commit()
			if err != nil {
				t.Error(err)
				return
			}

			row := handler.db.QueryRow("SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND column_name = 'time'AND table_name = '" + table + "';")
			dataType := ""
			err = row.Scan(&dataType)
			if err != nil {
				t.Error(err)
				return
			}
			if dataType != "timestamp without time zone" {
				t.Error("time column migration test could not be run, timestamp is already with time zone")
				return
			}
			err = handler.migrateTIMESTAMP_TIMESTAMPTZ()
			if err != nil {
				t.Error(err)
				return
			}
			row = handler.db.QueryRow("SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND column_name = 'time'AND table_name = '" + table + "';")
			err = row.Scan(&dataType)
			if err != nil {
				t.Error(err)
				return
			}
			if dataType != "timestamp with time zone" {
				t.Errorf("time column not migrated, expected type 'timestamp with timezone', got '%v'", dataType)
				return
			}
			validate(t)
		})

		t.Run("still writable", func(t *testing.T) {
			_, err = handler.db.Exec("INSERT INTO \"" + table + "\"(\"measurements.measurement.value\", \"measurements.measurement.value3\", \"measurements.listvariable\", \"measurements.listfixed.0.value\", \"measurements.listfixed.0.value2\", \"measurements.listfixed.1.value\", \"measurements.listfixed.1.value2\", time) VALUES (0, 1, '[{\"value\": 12, \"value2\": 34}, {\"value\": 56, \"value2\": 78}, {\"value\": 90, \"value2\": 12}]', 12, 34, 56, 78, '1970-01-01T00:00:00Z');")
			if err != nil {
				t.Error(err)
				return
			}
		})
	})

	t.Run("multi-part device creation", func(t *testing.T) {
		testDevicePut(t, models.Device{
			LocalId:      "multipart",
			Name:         "multipart",
			DeviceTypeId: multiPartDt.Id,
		}, now.Add(3*time.Second))
	})

	// columnstore tests: existing tests run with ColumnstoreAfter "", these enable it per subtest.
	// All test data lies in 1970/2020, so it counts as old for the policy.
	const columnstoreAfterTest = "14 days"
	withColumnstore := func(t *testing.T, after string) {
		old := handler.conf.ColumnstoreAfter
		handler.conf.ColumnstoreAfter = after
		t.Cleanup(func() { handler.conf.ColumnstoreAfter = old })
	}
	csVar := func(name string, typ models.Type) models.ContentVariable {
		return models.ContentVariable{
			Name:                 name,
			Type:                 typ,
			CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
			FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
			AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
			SerializationOptions: []string{models.SerializationOptionXmlAttribute},
		}
	}
	csService := func(serviceId string, vars ...models.ContentVariable) models.Service {
		return models.Service{
			LocalId:     "sensor",
			Name:        "sensor",
			Id:          serviceId,
			Interaction: models.EVENT,
			ProtocolId:  "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
			Outputs: []models.Content{{
				ContentVariable:   models.ContentVariable{Name: "m", Type: models.Structure, SubContentVariables: vars},
				Serialization:     models.XML,
				ProtocolSegmentId: "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65",
			}},
		}
	}
	csDt := func(service models.Service) models.DeviceType {
		return models.DeviceType{
			Name:          "columnstore-" + service.Id,
			Id:            "urn:infai:ses:device-type:" + uuid.NewString(),
			DeviceClassId: "urn:infai:ses:device-class:997937d6-c5f3-4486-b67c-114675038393",
			Attributes:    []models.Attribute{},
			Services:      []models.Service{service},
		}
	}
	csTableName := func(t *testing.T, deviceId string, serviceId string) string {
		shortDeviceId, err := models.ShortenId(deviceId)
		if err != nil {
			t.Fatal(err)
		}
		shortServiceId, err := models.ShortenId(serviceId)
		if err != nil {
			t.Fatal(err)
		}
		return "device:" + shortDeviceId + "_service:" + shortServiceId
	}
	// creates device type, device and table with columnstore enabled; returns dt, device id and table
	csSetup := func(t *testing.T, vars ...models.ContentVariable) (models.DeviceType, string, string) {
		service := csService("urn:infai:ses:service:"+uuid.NewString(), vars...)
		dt := testDtPut(t, csDt(service), now)
		deviceId := testDevicePut(t, models.Device{LocalId: "cs-" + uuid.NewString(), Name: "cs", DeviceTypeId: dt.Id}, now.Add(2*time.Second))
		table := csTableName(t, deviceId, service.Id)
		if t.Failed() {
			t.FailNow()
		}
		return dt, deviceId, table
	}
	csExec := func(t *testing.T, query string, args ...any) {
		t.Helper()
		_, err := handler.db.Exec(query, args...)
		if err != nil {
			t.Fatal(query, err)
		}
	}
	csInt := func(t *testing.T, query string, args ...any) (i int64) {
		t.Helper()
		err := handler.db.QueryRow(query, args...).Scan(&i)
		if err != nil {
			t.Fatal(query, err)
		}
		return i
	}
	csString := func(t *testing.T, query string, args ...any) (s string) {
		t.Helper()
		err := handler.db.QueryRow(query, args...).Scan(&s)
		if err != nil {
			t.Fatal(query, err)
		}
		return s
	}
	// the policy job of a new table is due immediately and may compress chunks concurrently, so "already compressed" is tolerated
	compressAll := func(t *testing.T, table string) {
		t.Helper()
		chunks := []string{}
		rows, err := handler.db.Query("SELECT c::text FROM show_chunks($1::regclass) c;", "\""+table+"\"")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var c string
			if err = rows.Scan(&c); err != nil {
				t.Fatal(err)
			}
			chunks = append(chunks, c)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		for _, c := range chunks {
			_, err = handler.db.Exec("SELECT compress_chunk($1::regclass, if_not_compressed => true);", c)
			if err != nil && !strings.Contains(err.Error(), "already compressed") {
				t.Fatal(err)
			}
		}
	}
	chunkCount := func(t *testing.T, table string, compressed bool) int64 {
		return csInt(t, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'public' AND hypertable_name = $1 AND is_compressed = $2;", table, compressed)
	}
	policyJobId := func(t *testing.T, table string) int64 {
		return csInt(t, "SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_schema = 'public' AND hypertable_name = $1;", table)
	}
	fillHourly := func(t *testing.T, table string, rows int) {
		csExec(t, "INSERT INTO \""+table+"\"(\"m.v\", \"m.w\", time) SELECT g, g * 2, timestamptz '2020-01-01T00:00:00Z' + g * interval '1 hour' FROM generate_series(0, $1::int - 1) g;", rows)
	}

	t.Run("columnstore on new table", func(t *testing.T) {
		withColumnstore(t, columnstoreAfterTest)
		_, _, table := csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		if csString(t, "SELECT compression_enabled::text FROM timescaledb_information.hypertables WHERE hypertable_schema = 'public' AND hypertable_name = $1;", table) != "true" {
			t.Error("compression not enabled")
		}
		if n := csInt(t, "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = $1;", table); n != 1 {
			t.Fatalf("expected exactly one policy_compression job, got %d", n)
		}
		if after := csString(t, "SELECT config->>'compress_after' FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = $1;", table); after != "14 days" {
			t.Errorf("unexpected compress_after %q", after)
		}
		if schedule := csString(t, "SELECT schedule_interval::text FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = $1;", table); schedule != "1 day" {
			t.Errorf("unexpected schedule_interval %q", schedule)
		}
		// enabling again must be idempotent
		tx, err := handler.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if err = handler.enableColumnstore(tx, table, nil); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if n := csInt(t, "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = $1;", table); n != 1 {
			t.Fatalf("expected exactly one policy_compression job after second call, got %d", n)
		}
	})

	t.Run("columnstore migration", func(t *testing.T) {
		withColumnstore(t, "")
		for i := 0; i < 3; i++ {
			csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		}
		deviceTables := "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema = 'public' AND hypertable_name ~ '^device:.{22}_service:.{22}$'"
		total := csInt(t, deviceTables+";")
		before := csInt(t, deviceTables+" AND NOT compression_enabled;")
		if before < 3 {
			t.Fatalf("expected at least 3 tables without compression, got %d", before)
		}
		// feature off: no-op
		if err := handler.migrate(); err != nil {
			t.Fatal(err)
		}
		if n := csInt(t, deviceTables+" AND NOT compression_enabled;"); n != before {
			t.Fatalf("migration must be a no-op without ColumnstoreAfter, %d != %d", n, before)
		}

		handler.conf.ColumnstoreAfter = columnstoreAfterTest
		started := time.Now()
		if err := handler.migrate(); err != nil {
			t.Fatal(err)
		}
		if n := csInt(t, deviceTables+" AND NOT compression_enabled;"); n != 0 {
			t.Fatalf("%d tables not converted", n)
		}
		jobQuery := "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name ~ '^device:.{22}_service:.{22}$'"
		if n := csInt(t, jobQuery+";"); n != total {
			t.Fatalf("expected %d policy jobs, got %d", total, n)
		}
		// initial_start spread within the window [start, start+6h]
		if n := csInt(t, jobQuery+" AND (initial_start < $1 OR initial_start > $1::timestamptz + interval '6 hours' + interval '1 minute');", started.Add(-time.Minute)); n != 0 {
			t.Fatalf("%d jobs with initial_start outside the spread window", n)
		}
		// tables converted by the migration have a distinct fixed initial_start each (the table from the earlier subtest has none)
		if n := csInt(t, "SELECT count(DISTINCT initial_start) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name ~ '^device:.{22}_service:.{22}$';"); n != before {
			t.Errorf("expected %d distinct initial_start values, got %d", before, n)
		}
		snapshot := func() string {
			return csString(t, "SELECT string_agg(job_id::text || '/' || initial_start::text || '/' || next_start::text, ',' ORDER BY job_id) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression';")
		}
		s1 := snapshot()
		if err := handler.migrate(); err != nil {
			t.Fatal(err)
		}
		if s2 := snapshot(); s1 != s2 {
			t.Errorf("second migration changed jobs:\n%s\n%s", s1, s2)
		}

		// device table with columnstore but without policy gets exactly one policy; a paused policy is left alone
		_, _, noPolicy := csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		csExec(t, "ALTER TABLE \""+noPolicy+"\" SET (timescaledb.enable_columnstore = true, timescaledb.segmentby = '', timescaledb.orderby = '\"time\" DESC');")
		_, _, paused := csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		tx, err := handler.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if err = handler.enableColumnstore(tx, paused, nil); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		csExec(t, "SELECT alter_job($1, scheduled => false);", policyJobId(t, paused))
		// a table that is locked during the first pass(es) is retried
		_, _, locked := csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		lockTx, err := handler.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = lockTx.Exec("LOCK TABLE \"" + locked + "\" IN ACCESS SHARE MODE;"); err != nil {
			_ = lockTx.Rollback()
			t.Fatal(err)
		}
		// timing: pass 1 gives up after the 2s lock timeout, pass 2 starts right after and finishes the table once the lock is gone at 3s
		released := make(chan struct{})
		go func() {
			defer close(released)
			time.Sleep(3 * time.Second)
			_ = lockTx.Rollback()
		}()
		retryStart := time.Now()
		if err = handler.migrate(); err != nil {
			t.Fatal(err)
		}
		<-released
		if time.Since(retryStart) < 2*time.Second {
			t.Error("migration did not wait for the lock, retry path not exercised")
		}
		if n := csInt(t, "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = $1;", noPolicy); n != 1 {
			t.Errorf("table without policy: expected exactly one policy job, got %d", n)
		}
		if n := csInt(t, "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = $1;", paused); n != 1 {
			t.Errorf("table with paused policy: expected exactly one policy job, got %d", n)
		}
		if scheduled := csString(t, "SELECT scheduled::text FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = $1;", paused); scheduled != "false" {
			t.Error("paused policy was touched")
		}
		if n := csInt(t, "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = $1;", locked); n != 1 {
			t.Errorf("locked table not converted by the retry, policy jobs: %d", n)
		}
	})

	t.Run("columnstore type change with compressed chunks", func(t *testing.T) {
		withColumnstore(t, columnstoreAfterTest)
		dt, deviceId, table := csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		fillHourly(t, table, 42*24)
		csExec(t, "CREATE MATERIALIZED VIEW \""+table+"_ld\" WITH (timescaledb.continuous) AS SELECT time_bucket(INTERVAL '1 day', time) AS time, last(\"m.v\", time) AS \"m.v\", last(\"m.w\", time) AS \"m.w\" FROM \""+table+"\" GROUP BY time_bucket(INTERVAL '1 day', time) WITH NO DATA;")
		csExec(t, "CALL refresh_continuous_aggregate('\""+table+"_ld\"', NULL, NULL);")
		caRows := csInt(t, "SELECT count(*) FROM \""+table+"_ld\";")
		caSum := csInt(t, "SELECT sum(\"m.v\") FROM \""+table+"_ld\";")
		compressAll(t, table)
		if n := chunkCount(t, table, true); n < 5 {
			t.Fatalf("expected several compressed chunks, got %d", n)
		}
		rowsBefore := csInt(t, "SELECT count(*) FROM \""+table+"\";")
		sumBefore := csInt(t, "SELECT sum(\"m.v\") FROM \""+table+"\";")

		// bigint -> double precision, drop w, add x
		dt.Services[0] = csService(dt.Services[0].Id, csVar("v", models.Float), csVar("x", models.Integer))
		testDtPut(t, dt, now.Add(24*time.Hour))
		if t.Failed() {
			t.FailNow()
		}
		if n := csInt(t, "SELECT count(*) FROM \""+table+"\";"); n != rowsBefore {
			t.Errorf("row count changed: %d != %d", n, rowsBefore)
		}
		if typ := csString(t, "SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'm.v';", table); typ != "double precision" {
			t.Errorf("unexpected type %q", typ)
		}
		if sum := csInt(t, "SELECT sum(\"m.v\")::bigint FROM \""+table+"\";"); sum != sumBefore {
			t.Errorf("values changed: %d != %d", sum, sumBefore)
		}
		if n := csInt(t, "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name IN ('m.x');", table); n != 1 {
			t.Error("added column missing")
		}
		if n := csInt(t, "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name IN ('m.w');", table); n != 0 {
			t.Error("removed column still present")
		}
		// the policy compresses again
		if n := chunkCount(t, table, true); n != 0 {
			t.Errorf("expected no compressed chunk right after the type change, got %d", n)
		}
		if n := csInt(t, "SELECT count(*) FROM timescaledb_information.continuous_aggregates WHERE view_name = $1;", table+"_ld"); n != 1 {
			t.Fatal("continuous aggregate not re-created")
		}
		if n := csInt(t, "SELECT count(*) FROM \""+table+"_ld\";"); n != caRows {
			t.Errorf("continuous aggregate rows %d != %d", n, caRows)
		}
		if n := csInt(t, "SELECT sum(\"m.v\")::bigint FROM \""+table+"_ld\";"); n != caSum {
			t.Errorf("continuous aggregate values %d != %d", n, caSum)
		}
		if n := csInt(t, "SELECT count(*) FROM \""+handler.conf.PostgresTableworkerSchema+"\".\""+tableUpdateBackups+"\" WHERE "+fieldDeviceId+" = $1;", deviceId); n != 0 {
			t.Errorf("%d update_backups rows left", n)
		}
		if n := csInt(t, "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name LIKE 'backup\\_%';", handler.conf.PostgresTableworkerSchema); n != 0 {
			t.Errorf("%d backup tables left", n)
		}
		// columnstore stays enabled and the policy compresses again
		if csString(t, "SELECT compression_enabled::text FROM timescaledb_information.hypertables WHERE hypertable_name = $1;", table) != "true" {
			t.Error("compression no longer enabled")
		}
		csExec(t, "CALL run_job($1);", policyJobId(t, table))
		if n := chunkCount(t, table, false); n != 0 {
			t.Errorf("policy left %d chunks uncompressed", n)
		}
	})

	t.Run("columnstore handleColumnTypeChange with compressed chunks", func(t *testing.T) {
		withColumnstore(t, columnstoreAfterTest)
		_, _, table := csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		fillHourly(t, table, 21*24)
		csExec(t, "CREATE MATERIALIZED VIEW \""+table+"_ld\" WITH (timescaledb.continuous) AS SELECT time_bucket(INTERVAL '1 day', time) AS time, last(\"m.v\", time) AS \"m.v\", last(\"m.w\", time) AS \"m.w\" FROM \""+table+"\" GROUP BY time_bucket(INTERVAL '1 day', time) WITH NO DATA;")
		csExec(t, "CALL refresh_continuous_aggregate('\""+table+"_ld\"', NULL, NULL);")
		compressAll(t, table)
		rowsBefore := csInt(t, "SELECT count(*) FROM \""+table+"\";")
		caRows := csInt(t, "SELECT count(*) FROM \""+table+"_ld\";")
		// m.v is used by a view: ALTER fails first and takes the savepoint workaround path
		tx, err := handler.db.BeginTx(handler.ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		tx, err = handler.handleColumnTypeChange(tx, table, fieldDescription{ColumnName: "\"m.v\"", Nullable: true, DataType: "double precision"}, handler.ctx, table)
		if err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if n := csInt(t, "SELECT count(*) FROM \""+table+"\";"); n != rowsBefore {
			t.Errorf("row count changed: %d != %d", n, rowsBefore)
		}
		if n := csInt(t, "SELECT count(*) FROM \""+table+"_ld\";"); n != caRows {
			t.Errorf("continuous aggregate rows %d != %d", n, caRows)
		}
		if typ := csString(t, "SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'm.v';", table); typ != "double precision" {
			t.Errorf("unexpected type %q", typ)
		}
	})

	t.Run("columnstore add, drop, not null keep chunks compressed", func(t *testing.T) {
		withColumnstore(t, columnstoreAfterTest)
		dt, _, table := csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		fillHourly(t, table, 42*24)
		compressAll(t, table)
		compressed := chunkCount(t, table, true)
		if compressed < 5 {
			t.Fatalf("expected several compressed chunks, got %d", compressed)
		}
		rowsBefore := csInt(t, "SELECT count(*) FROM \""+table+"\";")

		// add x, drop w through a device type update
		dt.Services[0] = csService(dt.Services[0].Id, csVar("v", models.Integer), csVar("x", models.Integer))
		testDtPut(t, dt, now.Add(24*time.Hour))
		if t.Failed() {
			t.FailNow()
		}
		if n := chunkCount(t, table, true); n != compressed {
			t.Errorf("ADD/DROP COLUMN changed compressed chunks: %d != %d", n, compressed)
		}
		if n := chunkCount(t, table, false); n != 0 {
			t.Errorf("%d chunks uncompressed after ADD/DROP COLUMN", n)
		}
		if n := csInt(t, "SELECT count(*) FROM \""+table+"\";"); n != rowsBefore {
			t.Errorf("row count changed: %d != %d", n, rowsBefore)
		}

		csExec(t, "ALTER TABLE \""+table+"\" ALTER COLUMN \"m.v\" SET NOT NULL;")
		if n := chunkCount(t, table, true); n != compressed {
			t.Errorf("SET NOT NULL changed compressed chunks: %d != %d", n, compressed)
		}
		csExec(t, "ALTER TABLE \""+table+"\" ALTER COLUMN \"m.v\" DROP NOT NULL;")
		if n := chunkCount(t, table, true); n != compressed {
			t.Errorf("DROP NOT NULL changed compressed chunks: %d != %d", n, compressed)
		}
	})

	t.Run("columnstore recompresses late writes", func(t *testing.T) {
		withColumnstore(t, columnstoreAfterTest)
		_, _, table := csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		fillHourly(t, table, 42*24)
		compressAll(t, table)
		rowsBefore := csInt(t, "SELECT count(*) FROM \""+table+"\";")
		statusQuery := "SELECT count(*) FROM _timescaledb_catalog.chunk c JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id WHERE h.table_name = $1 AND c.status & 8 = 8;"
		if n := csInt(t, statusQuery, table); n != 0 {
			t.Fatalf("%d chunks partial before late write", n)
		}
		csExec(t, "INSERT INTO \""+table+"\"(\"m.v\", \"m.w\", time) SELECT g, g, timestamptz '2020-01-02T00:30:00Z' + g * interval '1 minute' FROM generate_series(0, 99) g;")
		if n := csInt(t, statusQuery, table); n != 1 {
			t.Fatalf("expected exactly one partially compressed chunk, got %d", n)
		}
		if n := csInt(t, "SELECT count(*) FROM \""+table+"\";"); n != rowsBefore+100 {
			t.Fatalf("row count %d != %d", n, rowsBefore+100)
		}
		csExec(t, "CALL run_job($1);", policyJobId(t, table))
		if n := csInt(t, statusQuery, table); n != 0 {
			t.Errorf("policy left %d chunks partially compressed", n)
		}
		if n := chunkCount(t, table, false); n != 0 {
			t.Errorf("policy left %d chunks uncompressed", n)
		}
		if n := csInt(t, "SELECT count(*) FROM \""+table+"\";"); n != rowsBefore+100 {
			t.Errorf("row count after recompression %d != %d", n, rowsBefore+100)
		}
	})

	t.Run("columnstore size is visible in hypertable_approximate_size", func(t *testing.T) {
		withColumnstore(t, columnstoreAfterTest)
		_, _, table := csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		// per-minute data over 6 weeks: few, large chunks, so that the fixed per-chunk overhead does not dominate
		csExec(t, "INSERT INTO \""+table+"\"(\"m.v\", \"m.w\", time) SELECT g, g % 100, timestamptz '2020-01-01T00:00:00Z' + g * interval '1 minute' FROM generate_series(0, 42*24*60 - 1) g;")
		csExec(t, "ANALYZE \""+table+"\";")
		uncompressed := csInt(t, "SELECT hypertable_approximate_size($1::regclass);", "\""+table+"\"")
		compressAll(t, table)
		approx := csInt(t, "SELECT hypertable_approximate_size($1::regclass);", "\""+table+"\"")
		stats := csInt(t, "SELECT after_compression_total_bytes FROM hypertable_columnstore_stats($1::regclass);", "\""+table+"\"")
		t.Logf("SIZE-NUMBERS uncompressed=%d approximate_after_compression=%d after_compression_total_bytes=%d", uncompressed, approx, stats)
		if approx >= uncompressed {
			t.Errorf("approximate size did not shrink: %d >= %d", approx, uncompressed)
		}
		// approximate_size also counts the empty chunk stubs (heap and indexes, a few KB each) that after_compression_total_bytes leaves out
		chunks := chunkCount(t, table, true)
		if approx < stats || approx > stats+chunks*16*1024 {
			t.Errorf("approximate size %d not within compressed size %d plus %d chunk stubs", approx, stats, chunks)
		}
	})

	t.Run("columnstore on a table in the production state", func(t *testing.T) {
		withColumnstore(t, "")
		_, _, table := csSetup(t, csVar("v", models.Integer), csVar("w", models.Integer))
		// 90 days of hourly data ending now
		csExec(t, "INSERT INTO \""+table+"\"(\"m.v\", \"m.w\", time) SELECT g, g * 2, now() - interval '90 days' + g * interval '1 hour' FROM generate_series(0, 90 * 24 - 1) g;")
		// continuous aggregate as created by the last-daily.json rule-manager template
		csExec(t, "CREATE MATERIALIZED VIEW IF NOT EXISTS \""+table+"_ld\" WITH (timescaledb.continuous) AS SELECT time_bucket(INTERVAL '1 day', time, 'Europe/Berlin') AS time, last(\"m.v\", time) AS \"m.v\", last(\"m.w\", time) AS \"m.w\" FROM \""+table+"\" GROUP BY 1 WITH NO DATA;")
		csExec(t, "ALTER MATERIALIZED VIEW \""+table+"_ld\" set (timescaledb.materialized_only = false);")
		csExec(t, "SELECT remove_continuous_aggregate_policy('\""+table+"_ld\"', if_exists => true);")
		csExec(t, "SELECT add_continuous_aggregate_policy('\""+table+"_ld\"', start_offset => NULL, end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 day', if_not_exists => true);")
		csExec(t, "SELECT add_retention_policy('\""+table+"\"', INTERVAL '1 year');")
		csExec(t, "CALL refresh_continuous_aggregate('\""+table+"_ld\"', NULL, NULL);")
		rawRows := csInt(t, "SELECT count(*) FROM \""+table+"\";")

		handler.conf.ColumnstoreAfter = columnstoreAfterTest
		if err := handler.migrate(); err != nil {
			t.Fatal(err)
		}
		if csString(t, "SELECT compression_enabled::text FROM timescaledb_information.hypertables WHERE hypertable_name = $1;", table) != "true" {
			t.Fatal("migration did not enable compression")
		}
		compressionJob := policyJobId(t, table)
		csExec(t, "CALL run_job($1);", compressionJob)
		if n := csInt(t, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = $1 AND range_end < now() - interval '14 days' AND NOT is_compressed;", table); n != 0 {
			t.Errorf("%d old chunks not compressed", n)
		}
		if n := csInt(t, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = $1 AND range_end > now() - interval '14 days' AND is_compressed;", table); n != 0 {
			t.Errorf("%d young chunks compressed", n)
		}
		oldChunks := chunkCount(t, table, true)
		if oldChunks < 5 || chunkCount(t, table, false) < 1 {
			t.Fatalf("expected compressed and uncompressed chunks, got %d compressed, %d uncompressed", oldChunks, chunkCount(t, table, false))
		}

		// late writes into compressed chunks, a second after the original row, so that the daily last value changes in every bucket
		csExec(t, "INSERT INTO \""+table+"\"(\"m.v\", \"m.w\", time) SELECT \"m.v\" + 1000000, \"m.w\" + 1000000, time + interval '1 second' FROM \""+table+"\" WHERE time >= now() - interval '61 days' AND time < now() - interval '59 days';")
		partialQuery := "SELECT count(*) FROM _timescaledb_catalog.chunk c JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id WHERE h.table_name = $1 AND c.status & 8 = 8;"
		if n := csInt(t, partialQuery, table); n < 1 {
			t.Errorf("expected partially compressed chunks after late writes, got %d", n)
		}
		if n := csInt(t, "SELECT count(*) FROM \""+table+"\";"); n <= rawRows {
			t.Fatalf("late writes missing, %d rows", n)
		}
		diff := "SELECT count(*) FROM ((SELECT time, \"m.v\", \"m.w\" FROM \"" + table + "_ld\" EXCEPT SELECT time_bucket(INTERVAL '1 day', time, 'Europe/Berlin'), last(\"m.v\", time), last(\"m.w\", time) FROM \"" + table + "\" GROUP BY 1) UNION ALL (SELECT time_bucket(INTERVAL '1 day', time, 'Europe/Berlin'), last(\"m.v\", time), last(\"m.w\", time) FROM \"" + table + "\" GROUP BY 1 EXCEPT SELECT time, \"m.v\", \"m.w\" FROM \"" + table + "_ld\")) d;"
		if n := csInt(t, diff); n == 0 {
			t.Error("continuous aggregate already equals the raw aggregation before its refresh, late writes had no effect")
		}
		caJob := csInt(t, "SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_refresh_continuous_aggregate' AND (config->>'mat_hypertable_id')::int = (SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = $1);", table+"_ld")
		// the policy of a new continuous aggregate is due immediately; wait for its background run, if it is still going
		for i := 0; ; i++ {
			_, err := handler.db.Exec("CALL run_job($1);", caJob)
			if err == nil {
				break
			}
			if !strings.Contains(err.Error(), "concurrent refresh") || i >= 50 {
				t.Fatal(err)
			}
			time.Sleep(200 * time.Millisecond)
		}
		if n := csInt(t, diff); n != 0 {
			t.Errorf("continuous aggregate differs from the raw aggregation in %d rows after its refresh", n)
		}

		csExec(t, "CALL run_job($1);", compressionJob)
		if n := csInt(t, partialQuery, table); n != 0 {
			t.Errorf("%d chunks still partially compressed", n)
		}
		if n := chunkCount(t, table, true); n < oldChunks {
			t.Errorf("compressed chunks %d < %d", n, oldChunks)
		}
		if n := csInt(t, diff); n != 0 {
			t.Errorf("continuous aggregate differs from the raw aggregation in %d rows after recompression", n)
		}
	})
}

type testProducer struct {
	f func(topic string, msg string) error
}

func (this *testProducer) Publish(topic string, msg string) error {
	return this.f(topic, msg)
}
