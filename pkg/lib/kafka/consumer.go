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

package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func NewConsumer(ctx context.Context, wg *sync.WaitGroup, kafkaBootstrap string, topics []string, groupId string, listener func(topic string, msg []byte, time time.Time) error, errorhandler func(err error, consumer *Consumer), debug bool) (consumer *Consumer, needsSync bool, err error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	brokers := strings.Split(kafkaBootstrap, ",")

	client, err := sarama.NewClient(brokers, saramaConfig)
	if err != nil {
		return nil, false, fmt.Errorf("Error creating client: %v", err)
	}

	earliestOffset := map[string]map[int32]int64{}
	topicPartitions := map[string][]int32{}

	for _, topic := range topics {
		earliestOffset[topic] = map[int32]int64{}
		topicPartitions[topic] = []int32{}
		partitions, err := client.Partitions(topic)
		if err != nil {
			return nil, false, fmt.Errorf("Error getting partitions: %v", err)
		}
		for _, partition := range partitions {
			offset, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				return nil, false, fmt.Errorf("Error getting offset: %v", err)
			}
			earliestOffset[topic][partition] = offset
			topicPartitions[topic] = append(topicPartitions[topic], partition)
		}
	}
	admin, err := sarama.NewClusterAdmin(brokers, saramaConfig)
	if err != nil {
		return nil, false, fmt.Errorf("Error getting kafka cluster admin: %v", err)
	}
	offsets, err := admin.ListConsumerGroupOffsets(groupId, topicPartitions)
	if err != nil {
		return nil, false, fmt.Errorf("Error getting kafka consumer group offsets: %v", err)
	}
	checked := false
outer:
	for topic, topicMeta := range offsets.Blocks {
		earliestOffsetTopic, ok := earliestOffset[topic]
		if !ok {
			needsSync = true
			break outer
		}
		for partition, partitionMeta := range topicMeta {
			checked = true
			earliestStoredOffset, ok := earliestOffsetTopic[partition]
			if !ok || partitionMeta.Offset < earliestStoredOffset {
				needsSync = true
				break outer
			}
		}
	}
	if !checked {
		needsSync = true
	}

	consumer = &Consumer{ctx: ctx, wg: wg, brokers: brokers, topics: topics, listener: listener, errorhandler: errorhandler, ready: make(chan bool), groupId: groupId, debug: debug, saramaConfig: saramaConfig}
	return
}

type Consumer struct {
	count        int
	brokers      []string
	topics       []string
	ctx          context.Context
	wg           *sync.WaitGroup
	listener     func(topic string, msg []byte, time time.Time) error
	errorhandler func(err error, consumer *Consumer)
	mux          sync.Mutex
	offset       int64
	groupId      string
	ready        chan bool
	debug        bool
	saramaConfig *sarama.Config
}

func (this *Consumer) Start() error {
	consumerGroup, err := sarama.NewConsumerGroup(this.brokers, this.groupId, this.saramaConfig)
	if err != nil {
		return fmt.Errorf("Error creating consumer group client: %v", err)
	}

	go func() {
		for {
			select {
			case <-this.ctx.Done():
				log.Println("close kafka reader")
				return
			default:
				if err := consumerGroup.Consume(this.ctx, this.topics, this); err != nil {
					log.Panicf("Error from consumer: %v", err)
				}
				// check if context was cancelled, signaling that the consumer should stop
				if this.ctx.Err() != nil {
					return
				}
				this.ready = make(chan bool)
			}
		}
	}()

	<-this.ready // Await till the consumer has been set up
	log.Println("Kafka consumer up and running...")

	return err
}

func (this *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(this.ready)
	this.wg.Add(1)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (this *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Cleaned up kafka session")
	this.wg.Done()
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (this *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		select {
		case <-this.ctx.Done():
			log.Println("Ignoring queued kafka messages for faster shutdown")
			return nil
		default:
			if this.debug {
				log.Println(message.Topic, message.Timestamp, string(message.Value))
			}
			err := this.listener(message.Topic, message.Value, message.Timestamp)
			if err != nil {
				this.errorhandler(err, this)
			}
			session.MarkMessage(message, "")
		}
	}

	return nil
}
