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

package kafka

import (
	"context"
	"runtime/debug"
	"sync"
	"time"

	"github.com/SENERGY-Platform/timescale-tableworker/pkg/config"
	"github.com/Shopify/sarama"
)

type Producer struct {
	config       config.Config
	syncProducer sarama.SyncProducer
}

func NewProducer(conf config.Config, ctx context.Context, wg *sync.WaitGroup) (*Producer, error) {
	p := &Producer{config: conf}
	var err error
	p.syncProducer, err = p.ensureConnection()
	wg.Add(1)
	go func() {
		<-ctx.Done()
		if p.syncProducer != nil {
			_ = p.syncProducer.Close()
		}
		wg.Done()
	}()
	return p, err
}

func (producer *Producer) ensureConnection() (syncProducer sarama.SyncProducer, err error) {
	if producer.syncProducer != nil {
		return producer.syncProducer, nil
	}
	kafkaConf := sarama.NewConfig()
	kafkaConf.Producer.Flush.MaxMessages = 1
	kafkaConf.Producer.Return.Successes = true
	kafkaConf.Net.ReadTimeout = 120 * time.Second
	kafkaConf.Net.WriteTimeout = 120 * time.Second
	syncP, err := sarama.NewSyncProducer([]string{producer.config.KafkaBootstrap}, kafkaConf)
	if err != nil {
		producer.syncProducer = syncP
	}
	return syncP, err
}

func (producer *Producer) Publish(topic string, msg string) error {
	if producer.syncProducer == nil {
		var err error
		producer.syncProducer, err = producer.ensureConnection()
		if err != nil {
			return err
		}
	}
	_, _, err := producer.syncProducer.SendMessage(
		&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		})
	if err != nil {
		debug.PrintStack()
	}
	return err
}
