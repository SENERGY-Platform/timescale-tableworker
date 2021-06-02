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
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/kafka"
	"log"
	"runtime/debug"
	"sync"
)

func Start(ctx context.Context, config Config) (wg *sync.WaitGroup, err error) {
	wg = &sync.WaitGroup{}

	handler, err := NewHandler(config, wg, ctx)
	if err != nil {
		debug.PrintStack()
		return wg, err
	}

	var offset int64
	switch config.KafkaOffset {
	case "latest", "largest":
		offset = kafka.Latest
		break
	case "earliest", "smallest":
		offset = kafka.Earliest
		break
	default:
		log.Println("WARN: Unknown kafka offset. Use 'latest/largest' or 'earliest/smallest'. Using latest")
		offset = kafka.Latest
	}
	_, err = kafka.NewConsumer(ctx, wg, config.KafkaBootstrap, []string{config.KafkaTopic}, config.KafkaGroupId, offset, handler.handleMessage, handleError, config.Debug, config.KafkaCommitMessages)
	if err != nil {
		log.Fatal("ERROR: unable to start kafka connection ", err)
		return wg, err
	}

	return
}
