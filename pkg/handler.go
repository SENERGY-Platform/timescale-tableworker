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

package pkg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/devicetypes"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/kafka"
	_ "github.com/lib/pq"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Handler struct {
	db               *sql.DB
	distributed      bool
	replication      string
	deviceManagerUrl string
	debug            bool
	ctx              context.Context
}

type command string

const (
	putCommand    command = "PUT"
	deleteCommand command = "DELETE"
)

type device struct {
	Id           string `json:"id"`
	LocalId      string `json:"local_id"`
	Name         string `json:"name"`
	DeviceTypeId string `json:"device_type_id"`
}

type deviceCommand struct {
	Command command `json:"command"`
	Id      string  `json:"id"`
	Owner   string  `json:"owner"`
	Device  device  `json:"device"`
}

func NewHandler(c Config, wg *sync.WaitGroup, ctx context.Context) (handler *Handler, err error) {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", c.PostgresHost,
		c.PostgresPort, c.PostgresUser, c.PostgresPw, c.PostgresDb)
	log.Println("Connecting to PSQL...", psqlconn)
	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	wg.Add(1)
	go func() {
		<-ctx.Done()
		_ = db.Close()
		wg.Done()
	}()
	return &Handler{
		db:               db,
		distributed:      c.UseDistributedHypertables,
		replication:      strconv.Itoa(c.HypertableReplicationFactor),
		deviceManagerUrl: c.DeviceManagerUrl,
		debug:            c.Debug,
		ctx:              ctx,
	}, nil
}

func (handler *Handler) handleMessage(_ string, msg []byte, _ time.Time) error {
	var cmd deviceCommand
	err := json.Unmarshal(msg, &cmd)
	if err != nil {
		return err
	}
	switch cmd.Command {
	case putCommand:
		return handler.createTables(cmd.Device)
	case deleteCommand:
		return handler.deleteTables(cmd.Id)
	default:
		return errors.New("unknown command (ignored): " + string(cmd.Command))
	}
}

func handleError(err error, _ *kafka.Consumer) {
	log.Println(err)
}

func (handler *Handler) createTables(d device) error {
	dt, err := devicetypes.GetDeviceType(d.DeviceTypeId, handler.deviceManagerUrl)
	if err != nil {
		log.Println("Could not get device type")
		return err
	}
	for _, service := range dt.Services {
		shortDeviceId, err := devicetypes.ShortenId(d.Id)
		if err != nil {
			return err
		}
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
	}
	return nil
}

func (handler *Handler) deleteTables(id string) error {
	shortId, err := devicetypes.ShortenId(id)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(handler.ctx, time.Second*30)
	tx, err := handler.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		cancel()
		return err
	}
	query := "SELECT table_name FROM information_schema.tables WHERE table_name like 'device:" + shortId + "%';"
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

func parseContentVariable(c devicetypes.ContentVariable, path string) []string {
	s := []string{}
	prefix := path
	if len(prefix) > 0 {
		prefix += "."
	}
	prefix += c.Name
	switch c.Type {
	case devicetypes.String:
		s = append(s, "\""+prefix+"\" text NULL")
	case devicetypes.Boolean:
		s = append(s, "\""+prefix+"\" bool NULL")
	case devicetypes.Float:
		s = append(s, "\""+prefix+"\" double PRECISION NULL")
	case devicetypes.Integer:
		s = append(s, "\""+prefix+"\" int NULL")
	case devicetypes.Structure:
		for _, sub := range c.SubContentVariables {
			s = append(s, parseContentVariable(sub, prefix)...)
		}
	case devicetypes.List:
		log.Println("WARN: creating fields for list type not supported yet, skipping!")
	}
	return s
}
