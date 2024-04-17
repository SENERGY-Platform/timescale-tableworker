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

package docker

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Timescale(ctx context.Context, wg *sync.WaitGroup) (host string, port int, user string, pw string, db string, err error) {
	log.Println("start timescale")
	pw = "postgrespw"
	user = "postgres"
	db = "postgres"
	errStr := "The container name \"/timescale-wrapper-test-db\" is already in use"
	err = errors.New(errStr)
	i := 0
	var c testcontainers.Container
	for err != nil && strings.Contains(err.Error(), errStr) && i < 15 {
		i++
		c, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:           "timescale/timescaledb:2.8.1-pg13",
				Tmpfs:           map[string]string{},
				WaitingFor:      wait.ForListeningPort("5432/tcp"),
				ExposedPorts:    []string{"5432/tcp"},
				AlwaysPullImage: false,
				Env: map[string]string{
					"POSTGRES_PASSWORD": pw,
				},
				Name: "timescale-wrapper-test-db",
			},
			Started: true,
		})
		if err != nil {
			time.Sleep(time.Second)
		}
	}
	if err != nil {
		return host, port, user, pw, db, err
	}
	host, err = c.ContainerIP(ctx)
	if err != nil {
		return host, port, user, pw, db, err
	}
	port = 5432
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("DEBUG: remove container timescale", c.Terminate(context.Background()))
	}()

	return host, port, user, pw, db, err
}
