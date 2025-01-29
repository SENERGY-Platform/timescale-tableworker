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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/devicetypes"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

var caTypeRx = regexp.MustCompile(`,.*,\s+(.*\()`)

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
			return errors.Join(baseError, errors.New("could not obtain service meta"), err)
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
			return errors.Join(baseError, errors.New("could not obtain outdated device ids"), err)
		}
		if handler.debug {
			log.Printf("Found %v outdated devices that need to be updated\n", len(outdatedDeviceIds))
		}
		shortServiceId, err := devicetypes.ShortenId(service.Id)
		if err != nil {
			return errors.Join(baseError, errors.New("could not shorten service id"), err)
		}
		fdAfterAllChanges := getFieldDescriptions(service)
		created := []string{}
		for _, outdatedDeviceId := range outdatedDeviceIds {
			cnt := 0
			err = handler.db.QueryRow("SELECT COUNT(*) FROM \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableUpdateBackups + "\" WHERE " + fieldDeviceId + " = '" + outdatedDeviceId + "';").Scan(&cnt)
			if err != nil {
				return errors.Join(baseError, errors.New("could not check if backup data exists"), err)
			}
			if cnt > 0 {
				return errors.Join(baseError, errors.New("backup data exists. this is fishy, please manually fix it"))
			}
			shortDeviceId, err := devicetypes.ShortenId(outdatedDeviceId)
			if err != nil {
				return errors.Join(baseError, errors.New("could not obtain shortened device id"), err)
			}
			table := "device:" + shortDeviceId + "_service:" + shortServiceId
			exists, err := handler.tableExists(table)
			if err != nil {
				return errors.Join(baseError, errors.New("could not check if table exists "+table), err)
			}
			if !exists {
				if handler.debug {
					log.Printf("Table does not exist yet, creating now %v\n", table)
				}
				handler.createDeviceServiceTable(shortDeviceId, service)
			} else {
				if handler.debug {
					log.Printf("Table exists already, updating now %v\n", table)
				}
				ctx, cancel := context.WithTimeout(handler.ctx, 10*time.Minute)
				defer cancel() // cancel is also called at the end of the loop, deferring it in case of an early return
				tx, err := handler.db.BeginTx(ctx, &sql.TxOptions{})
				if err != nil {
					return errors.Join(baseError, errors.New("could not create transaction"), err)
				}
				fdBeforeAllChanges, err := getFieldDescriptionsOfTable(table, tx)
				if err != nil {
					return errors.Join(baseError, errors.New("could not obtain field descriptions"), err)
				}
				added, removed, newType, setNotNull, dropNotNull := compareFds(fdBeforeAllChanges, fdAfterAllChanges)
				for _, add := range added {
					fdCurrent, err := getFieldDescriptionsOfTable(table, tx)
					if err != nil {
						return errors.Join(baseError, errors.New("could not obtain field descriptions"), err)
					}
					currentFieldNames := []string{}
					for _, f := range fdCurrent {
						currentFieldNames = append(currentFieldNames, f.ColumnName)
					}

					query := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN %s;", table, add.String())
					if handler.debug {
						log.Println("Executing:", query)
					}
					_, err = tx.Exec(query)
					if err != nil {
						_ = tx.Rollback()
						return errors.Join(baseError, errors.New("could not execute query "+query), err)
					}

					err = forEachCAofHypertable(table, tx, func(hypertableName, viewSchema, viewName, viewDefinition string, materializedOnly bool) error {
						if handler.debug {
							log.Println("adding field " + add.ColumnName + " to view " + viewName)
						}
						_, err := handler.backupAndDropCA(outdatedDeviceId, viewSchema, viewName, viewDefinition, materializedOnly, tx)
						if err != nil {
							_ = tx.Rollback()
							return err
						}

						caTypeMatches := caTypeRx.FindStringSubmatch(viewDefinition)
						if len(caTypeMatches) != 2 {
							return errors.New("unexpected len(caTypeMatches)")
						}
						viewDefinitionParts := strings.Split(viewDefinition, "FROM")
						if len(viewDefinitionParts) != 2 {
							return errors.New("unexpected len(viewDefinitionParts)")
						}
						viewDefinition = viewDefinitionParts[0] + ", \n" + caTypeMatches[1] + "\"" + table + "\"." + add.ColumnName + ", \"" + table + "\".\"time\") AS " + add.ColumnName + "\n FROM" + viewDefinitionParts[1]
						err = createCA(viewSchema, viewName, viewDefinition, materializedOnly, tx)
						if err != nil {
							return err
						}
						return nil
					})
					// TX Commit needed, because following insertBackupDataAndDrop will not find the hypertable otherwise.
					// Since this might result in a partial update an ALL CAPS warning is printed and sent to dev notifications
					err = tx.Commit()
					if err != nil {
						return errors.Join(errors.New("could not commit transaction"), err)
					}
					tx, err = handler.db.BeginTx(ctx, &sql.TxOptions{})
					if err != nil {
						return errors.Join(errors.New("could not renew transaction, MIGHT NEED TO MANUALLY FIX WITH BACKUP DATA"), err)
					}

					err = handler.forEachStoredBackup(outdatedDeviceId, tx, func(backupTable, viewSchema, viewName, viewDefinition string, materializedOnly bool) error {
						err = handler.insertBackupDataAndDrop(viewSchema, viewName, backupTable, currentFieldNames, tx)
						if err != nil {
							return errors.Join(baseError, err)
						}
						return nil
					})
					if err != nil {
						_ = tx.Rollback()
						return errors.Join(baseError, err)
					}
				}
				for _, rm := range removed {
					savepoint := "\"" + uuid.NewString() + "\""
					_, err = tx.Exec("SAVEPOINT " + savepoint + ";")
					if err != nil {
						_ = tx.Rollback()
						return errors.Join(baseError, err)
					}
					query := fmt.Sprintf("ALTER TABLE \"%s\" DROP COLUMN %s;", table, rm.ColumnName) // can not use cascade as this simply drops all CAs
					if handler.debug {
						log.Println("Executing:", query)
					}
					_, err = tx.Exec(query)
					if err != nil {
						pqErr, ok := err.(*pq.Error)
						if ok && pqErr.Code == "2BP01" && pqErr.Message == "cannot drop desired object(s) because other objects depend on them" {
							_, err = tx.Exec("ROLLBACK TO SAVEPOINT " + savepoint + ";")
							if err != nil {
								_ = tx.Rollback()
								return errors.Join(baseError, err)
							}
							fdCurrent, err := getFieldDescriptionsOfTable(table, tx)
							if err != nil {
								return errors.Join(baseError, errors.New("could not obtain field descriptions"), err)
							}

							fieldNamesAfterThisRm := []string{}
							for _, f := range fdCurrent {
								if f.ColumnName != rm.ColumnName {
									fieldNamesAfterThisRm = append(fieldNamesAfterThisRm, f.ColumnName)
								}
							}

							rxStr := ",[^,]*(" + rm.ColumnName + ",.*" + rm.ColumnName + ")"
							rx, err := regexp.Compile(rxStr)
							if err != nil {
								return errors.Join(baseError, errors.New("unable to compile regexp "+rxStr), err)
							}

							err = forEachCAofHypertable(table, tx, func(hypertableName, viewSchema, viewName, viewDefinition string, materializedOnly bool) error {
								if handler.debug {
									log.Println("removing field " + rm.ColumnName + " from view " + viewName)
								}
								_, err := handler.backupAndDropCA(outdatedDeviceId, viewSchema, viewName, viewDefinition, materializedOnly, tx)
								if err != nil {
									_ = tx.Rollback()
									return err
								}
								viewDefinition = rx.ReplaceAllString(viewDefinition, "")
								err = createCA(viewSchema, viewName, viewDefinition, materializedOnly, tx)
								if err != nil {
									return err
								}

								return nil
							})
							if err != nil {
								_ = tx.Rollback()
								return errors.Join(baseError, err)
							}
							// TX Commit needed, because following insertBackupDataAndDrop will not find the hypertable otherwise.
							// Since this might result in a partial update an ALL CAPS warning is printed and sent to dev notifications
							err = tx.Commit()
							if err != nil {
								return errors.Join(errors.New("could not commit transaction"), err)
							}
							tx, err = handler.db.BeginTx(ctx, &sql.TxOptions{})
							if err != nil {
								return errors.Join(errors.New("could not renew transaction, MIGHT NEED TO MANUALLY FIX WITH BACKUP DATA"), err)
							}
							err = handler.forEachStoredBackup(outdatedDeviceId, tx, func(backupTable, viewSchema, viewName, viewDefinition string, materializedOnly bool) error {
								err = handler.insertBackupDataAndDrop(viewSchema, viewName, backupTable, fieldNamesAfterThisRm, tx)
								if err != nil {
									return errors.Join(baseError, err)
								}
								return nil
							})
							_, err = tx.Exec(query)
							if err != nil {
								_ = tx.Rollback()
								return errors.Join(baseError, err)
							}
						} else {
							_ = tx.Rollback()
							return errors.Join(baseError, errors.New("could not execute query "+query), err)
						}
					}
				}
				for _, nt := range newType {
					tx, err = handler.handleColumnTypeChange(tx, table, nt, ctx, outdatedDeviceId)
					if err != nil {
						_ = tx.Rollback()
						errors.Join(baseError, err)
					}
				}
				for _, nn := range setNotNull {
					query := fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN %s SET NOT NULL;", table, nn.ColumnName)
					if handler.debug {
						log.Println("Executing:", query)
					}
					_, err = tx.Exec(query)
					if err != nil {
						_ = tx.Rollback()
						return errors.Join(baseError, errors.New("could not execute query "+query), err)
					}
				}
				for _, nn := range dropNotNull {
					query := fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN %s DROP NOT NULL;", table, nn.ColumnName)
					if handler.debug {
						log.Println("Executing:", query)
					}
					_, err = tx.Exec(query)
					if err != nil {
						_ = tx.Rollback()
						return errors.Join(baseError, errors.New("could not execute query "+query), err)
					}
				}
				err = tx.Commit()
				if err != nil {
					_ = tx.Rollback()
					return errors.Join(baseError, errors.New("could not commit"), err)
				}
			}
			created = append(created, table)
		}
		err = handler.upsertServiceMeta(service.Id, newHash, t)
		if err != nil {
			return err
		}
		b, err := json.Marshal(TableEditMessage{
			Method: "put",
			Tables: created,
		})
		if err != nil {
			return err
		}
		err = handler.producer.Publish(handler.conf.KafkaTopicTableUpdates, string(b))
		if err != nil {
			return err
		}
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

type forEachFn = func(table, viewSchema, viewName, viewDefinition string, materializedOnly bool) error

func forEachCAofHypertable(table string, tx *sql.Tx, f forEachFn) error {
	query := "SELECT view_schema, view_name, materialized_only, view_definition FROM timescaledb_information.continuous_aggregates WHERE hypertable_name = '" + table + "';"
	res, err := tx.Query(query)
	if err != nil {
		_ = tx.Rollback()
		return errors.Join(errors.New("could not execute query "+query), err)
	}
	viewSchemas, viewNames, viewDefinitions := []string{}, []string{}, []string{}
	materializedOnlys := []bool{}
	for res.Next() {
		var viewSchema, viewName, viewDefinition string
		var materializedOnly bool
		err = res.Scan(&viewSchema, &viewName, &materializedOnly, &viewDefinition)
		if err != nil {
			return errors.Join(errors.New("could not scan view information"), err)
		}
		viewSchemas = append(viewSchemas, viewSchema)
		viewNames = append(viewNames, viewName)
		viewDefinitions = append(viewDefinitions, viewDefinition)
		materializedOnlys = append(materializedOnlys, materializedOnly)
	}
	for i := range viewSchemas {
		err = f(table, viewSchemas[i], viewNames[i], viewDefinitions[i], materializedOnlys[i])
		if err != nil {
			return errors.Join(errors.New("error running supplied function on view "+viewNames[i]), err)
		}
	}
	return nil
}

func (handler *Handler) forEachStoredBackup(deviceId string, tx *sql.Tx, f forEachFn) error {
	query := "SELECT " + strings.Join([]string{fieldViewSchema, fieldViewName, fieldBackupTable, fieldViewDefinition,
		fieldMaterializedOnly}, ",") + " FROM \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableUpdateBackups + "\" WHERE " + fieldDeviceId + " = '" + deviceId + "';"
	res, err := tx.Query(query)
	if err != nil {
		_ = tx.Rollback()
		return errors.Join(errors.New("could not execute query "+query), err)
	}
	viewSchemas, viewNames, viewDefinitions, backupTables := []string{}, []string{}, []string{}, []string{}
	materializedOnlys := []bool{}
	for res.Next() {
		var viewSchema, viewName, viewDefinition, backupTable string
		var materializedOnly bool
		err = res.Scan(&viewSchema, &viewName, &backupTable, &viewDefinition, &materializedOnly)
		if err != nil {
			return errors.Join(errors.New("could not scan backup information"), err)
		}
		viewDefinitionBytes, err := base64.StdEncoding.DecodeString(viewDefinition)
		if err != nil {
			return errors.Join(errors.New("could not decode view definition"), err)
		}
		viewDefinition = string(viewDefinitionBytes)

		viewSchemas = append(viewSchemas, viewSchema)
		viewNames = append(viewNames, viewName)
		viewDefinitions = append(viewDefinitions, viewDefinition)
		materializedOnlys = append(materializedOnlys, materializedOnly)
		backupTables = append(backupTables, backupTable)
	}
	for i := range viewSchemas {
		err = f(backupTables[i], viewSchemas[i], viewNames[i], viewDefinitions[i], materializedOnlys[i])
		if err != nil {
			return errors.Join(errors.New("error running supplied for each backup function for backup table "+backupTables[i]), err)
		}
	}
	return nil
}

func (handler *Handler) backupAndDropCA(deviceId, viewSchema, viewName, viewDefinition string, materializedOnly bool, tx *sql.Tx) (backupTable string, err error) {
	backupTable = "\"" + handler.conf.PostgresTableworkerSchema + "\".\"backup_" + strings.ReplaceAll(uuid.NewString(), "-", "") + "\""
	_, err = tx.Exec("CREATE TABLE " + backupTable + " as TABLE \"" + viewSchema + "\".\"" + viewName + "\";")
	if err != nil {
		return backupTable, errors.Join(errors.New("unable to create backup table"), err)
	}
	query := "INSERT INTO \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableUpdateBackups + "\" (" + strings.Join([]string{fieldDeviceId, fieldViewSchema, fieldViewName, fieldBackupTable, fieldViewDefinition,
		fieldMaterializedOnly}, ",") + ") VALUES ('" + deviceId + "','" + viewSchema + "','" + viewName + "','" + backupTable + "','" + base64.StdEncoding.EncodeToString([]byte(viewDefinition)) + "'," + strconv.FormatBool(materializedOnly) + ");"
	_, err = tx.Exec(query)
	if err != nil {
		return backupTable, errors.Join(errors.New("unable to insert backup information"), err)
	}
	_, err = tx.Exec("DROP MATERIALIZED VIEW \"" + viewSchema + "\".\"" + viewName + "\";")
	if err != nil {
		return backupTable, errors.Join(errors.New("unable to drop view"), err)
	}
	return
}

func createCA(viewSchema, viewName, viewDefinition string, materializedOnly bool, tx *sql.Tx) (err error) {
	_, err = tx.Exec("CREATE MATERIALIZED VIEW \"" + viewSchema + "\".\"" + viewName + "\"" +
		" WITH (timescaledb.continuous) AS " + viewDefinition[:len(viewDefinition)-1] + " WITH NO DATA;")
	if err != nil {
		return errors.Join(errors.New("unable to create new view"), err)
	}
	_, err = tx.Exec(fmt.Sprintf("ALTER MATERIALIZED VIEW \""+viewSchema+"\".\""+viewName+"\" SET (timescaledb.materialized_only = %v);", materializedOnly))
	if err != nil {
		return errors.Join(errors.New("unable to set materialized_only"), err)
	}
	return nil
}

func (handler *Handler) insertBackupDataAndDrop(viewSchema, viewName, backupTable string, fieldNames []string, tx *sql.Tx) (err error) {
	backupTableError := errors.New("MIGHT NEED TO MANUALLY FIX WITH BACKUP DATA FROM " + backupTable)

	// Need to find the underyling hypertable of the view to insert the backup data
	row := tx.QueryRow("SELECT materialization_hypertable_schema, materialization_hypertable_name FROM timescaledb_information.continuous_aggregates WHERE view_schema = '" + viewSchema + "' AND view_name = '" + viewName + "';")
	var materialization_hypertable_schema, materialization_hypertable_name string
	err = row.Scan(&materialization_hypertable_schema, &materialization_hypertable_name)
	if err != nil {
		return errors.Join(backupTableError, err)
	}

	fields := strings.Join(fieldNames, ", ")
	_, err = tx.Exec("INSERT INTO \"" + materialization_hypertable_schema + "\".\"" + materialization_hypertable_name + "\" (" + fields + ") SELECT " + fields + " FROM " + backupTable + ";")
	if err != nil {
		return errors.Join(backupTableError, errors.New("could not insert backup data"), err)
	}

	_, err = tx.Exec("DROP TABLE " + backupTable + ";")
	if err != nil {
		return errors.Join(errors.New("unable to delete backup table "+backupTable), err)
	}
	_, err = tx.Exec("DELETE FROM \"" + handler.conf.PostgresTableworkerSchema + "\".\"" + tableUpdateBackups + "\" WHERE " + fieldBackupTable + " = '" + backupTable + "';")
	if err != nil {
		return errors.Join(errors.New("unable to delete backup info of table "+backupTable), err)
	}
	return nil
}

func (handler *Handler) handleColumnTypeChange(tx *sql.Tx, table string, nt fieldDescription, ctx context.Context, identifier string) (*sql.Tx, error) {
	savepoint := "\"" + uuid.NewString() + "\""
	_, err := tx.Exec("SAVEPOINT " + savepoint + ";")
	if err != nil {
		_ = tx.Rollback()
		return tx, err
	}
	query := fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN %s TYPE %s;", table, nt.ColumnName, nt.DataType)
	if handler.debug {
		log.Println("Executing:", query)
	}
	_, err = tx.Query(query)
	if err != nil {
		pqErr, ok := err.(*pq.Error)
		if ok && pqErr.Code == "0A000" && pqErr.Message == "cannot alter type of a column used by a view or rule" {
			if handler.debug {
				log.Println("View is blocking change of column type, trying workaround")
			}
			_, err = tx.Exec("ROLLBACK TO SAVEPOINT " + savepoint + ";")
			if err != nil {
				return tx, err
			}
			fdCurrent, err := getFieldDescriptionsOfTable(table, tx)
			if err != nil {
				return tx, errors.Join(errors.New("could not obtain field descriptions"), err)
			}

			fieldNamesCurrent := []string{}
			for _, f := range fdCurrent {
				fieldNamesCurrent = append(fieldNamesCurrent, f.ColumnName)
			}

			err = forEachCAofHypertable(table, tx, func(hypertableName, viewSchema, viewName, viewDefinition string, materializedOnly bool) error {
				_, err := handler.backupAndDropCA(identifier, viewSchema, viewName, viewDefinition, materializedOnly, tx)
				if err != nil {
					_ = tx.Rollback()
					return err
				}
				return nil
			})
			if err != nil {
				_ = tx.Rollback()
				return tx, err
			}
			_, err = tx.Exec(query)
			if err != nil {
				return tx, err
			}
			err = handler.forEachStoredBackup(identifier, tx, func(backupTable, viewSchema, viewName, viewDefinition string, materializedOnly bool) error {
				err = createCA(viewSchema, viewName, viewDefinition, materializedOnly, tx)
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				_ = tx.Rollback()
				return tx, err
			}
			// TX Commit needed, because following insertBackupDataAndDrop will not find the hypertable otherwise.
			// Since this might result in a partial update an ALL CAPS warning is printed and sent to dev notifications
			err = tx.Commit()
			if err != nil {
				return tx, errors.Join(errors.New("could not commit transaction"), err)
			}
			tx, err = handler.db.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				return tx, errors.Join(errors.New("could not renew transaction, MIGHT NEED TO MANUALLY FIX WITH BACKUP DATA"), err)
			}

			err = handler.forEachStoredBackup(identifier, tx, func(backupTable, viewSchema, viewName, viewDefinition string, materializedOnly bool) error {
				return handler.insertBackupDataAndDrop(viewSchema, viewName, backupTable, fieldNamesCurrent, tx)
			})

			if err != nil {
				_ = tx.Rollback()
				return tx, err
			}

		} else {
			_ = tx.Rollback()
			return tx, errors.Join(errors.New("could not alter column type"), err)
		}
	}
	return tx, nil
}
