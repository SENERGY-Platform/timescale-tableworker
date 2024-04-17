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
	"context"
	"database/sql"
	"errors"
	"fmt"
	devNotifications "github.com/SENERGY-Platform/developer-notifications/pkg/client"
	"github.com/SENERGY-Platform/developer-notifications/pkg/model"
	"github.com/SENERGY-Platform/device-repository/lib/client"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/config"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/kafka"
	_ "github.com/lib/pq"
	"log"
	"strconv"
	"sync"
	"time"
)

type Handler struct {
	db                     *sql.DB
	distributed            bool
	replication            string
	deviceRepo             DeviceRepo
	debug                  bool
	ctx                    context.Context
	conf                   config.Config
	producer               Publisher
	devNotificationsClient devNotifications.Client
}

const (
	tableServiceHashes     string = "service_hashes"
	fieldServiceId         string = "service_id"
	fieldHash              string = "hash"
	tableDeviceTypeDevices string = "device_type_devices"
	fieldDeviceId          string = "device_id"
	fieldDeviceTypeId      string = "device_type_id"
	fieldTime              string = "time"
	tableUpdateBackups     string = "update_backups"
	fieldViewSchema        string = "view_schema"
	fieldViewName          string = "view_name"
	fieldBackupTable       string = "backup_table"
	fieldViewDefinition    string = "view_definition"
	fieldMaterializedOnly  string = "materialized_only"
)

type Publisher interface {
	Publish(topic string, msg string) error
}

type DeviceRepo interface {
	ReadDeviceType(id string, token string) (result models.DeviceType, err error, errCode int)
}

func NewHandler(c config.Config, wg *sync.WaitGroup, ctx context.Context) (handler *Handler, err error) {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", c.PostgresHost,
		c.PostgresPort, c.PostgresUser, c.PostgresPw, c.PostgresDb)
	log.Println("Connecting to PSQL...", psqlconn)
	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return nil, err
	}

	wg.Add(1)
	go func() {
		<-ctx.Done()
		_ = db.Close()
		wg.Done()
	}()

	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	producer, err := kafka.NewProducer(c, ctx, wg)
	if err != nil {
		return nil, err
	}

	handler = &Handler{
		db:                     db,
		distributed:            c.UseDistributedHypertables,
		replication:            strconv.Itoa(c.HypertableReplicationFactor),
		deviceRepo:             client.NewClient(c.DeviceManagerUrl),
		debug:                  c.Debug,
		ctx:                    ctx,
		conf:                   c,
		producer:               producer,
		devNotificationsClient: devNotifications.New(c.DevNotificationsUrl),
	}
	err = handler.initMetadataSchema()
	return
}

func (handler *Handler) HandleMessage(topic string, msg []byte, t time.Time) error {
	switch topic {
	case handler.conf.KafkaTopicDevices:
		return handler.handleDeviceMessage(msg, t)
	case handler.conf.KafkaTopicDeviceTypes:
		return handler.handleDeviceTypeMessage(msg, t)
	default:
		return errors.New("unknown topic (ignored): " + topic)
	}
}

func (handler *Handler) HandleError(err error, _ *kafka.Consumer) {
	log.Println(err)
	sendErr := handler.devNotificationsClient.SendMessage(model.Message{
		Sender: "timescale-tableworker",
		Title:  "Error handling table updates",
		Body:   err.Error(),
		Tags:   nil,
	})
	if sendErr != nil {
		log.Println("WARNING: Could not send developer-notification: " + sendErr.Error())
	}
}

func (handler *Handler) initMetadataSchema() error {
	query := "CREATE SCHEMA IF NOT EXISTS " + handler.conf.PostgresTableworkerSchema + ";"
	if handler.debug {
		log.Println(query)
	}
	_, err := handler.db.Exec(query)
	if err != nil {
		return err
	}

	query = "CREATE TABLE IF NOT EXISTS \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableServiceHashes + "\" (" +
		fieldServiceId + " text PRIMARY KEY, " +
		fieldHash + " text NOT NULL, " +
		fieldTime + " TIMESTAMP NOT NULL" +
		");"
	if handler.debug {
		log.Println(query)
	}
	_, err = handler.db.Exec(query)
	if err != nil {
		return err
	}

	query = "CREATE TABLE IF NOT EXISTS \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableDeviceTypeDevices + "\" (" +
		fieldDeviceTypeId + " text NOT NULL, " +
		fieldDeviceId + " text PRIMARY KEY, " +
		fieldTime + " TIMESTAMP NOT NULL" +
		");"
	if handler.debug {
		log.Println(query)
	}
	_, err = handler.db.Exec(query)
	if err != nil {
		return err
	}

	query = "CREATE TABLE IF NOT EXISTS \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableUpdateBackups + "\" (" +
		fieldDeviceId + " text NOT NULL, " +
		fieldViewSchema + " text NOT NULL, " +
		fieldViewName + " text NOT NULL, " +
		fieldBackupTable + " text NOT NULL, " +
		fieldViewDefinition + " text NOT NULL, " +
		fieldMaterializedOnly + " bool NOT NULL, " +
		"PRIMARY KEY (" + fieldDeviceId + ", " + fieldViewSchema + ", " + fieldViewName + ")" +
		");"
	if handler.debug {
		log.Println(query)
	}
	_, err = handler.db.Exec(query)
	if err != nil {
		return err
	}

	return nil
}
