// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"io"
	"time"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/kafka-topic-backup/avro"
)

func NewTopicRestorer(
	saramaClient libkafka.SaramaClient,
	opener Opener,
) *TopicRestorer {
	return &TopicRestorer{
		saramaClient: saramaClient,
		opener:       opener,
	}
}

type TopicRestorer struct {
	saramaClient libkafka.SaramaClient
	opener       Opener
}

func (t *TopicRestorer) Restore(
	ctx context.Context,
	backupDate libtime.Date,
	sourceTopic libkafka.Topic,
	targetTopic libkafka.Topic,
) error {
	// Open topic.avro file
	reader, err := t.opener.OpenReader(ctx, backupDate, sourceTopic, BackupTypeTopic)
	if err != nil {
		return errors.Wrap(ctx, err, "open topic backup failed")
	}
	defer reader.Close()

	// Create Kafka producer
	if t.saramaClient == nil {
		return errors.Wrap(ctx, errors.New(ctx, "sarama client is nil"), "create producer failed")
	}
	producer, err := sarama.NewSyncProducerFromClient(t.saramaClient)
	if err != nil {
		return errors.Wrap(ctx, err, "create producer failed")
	}
	defer producer.Close()

	// Read and restore messages
	messageCount := 0
	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx, ctx.Err(), "context cancelled during topic restore")
		default:
		}

		record, err := avro.DeserializeBackupTopicRecord(reader)
		if err != nil {
			if err == io.EOF {
				glog.V(2).Infof("restored %d messages to %s", messageCount, targetTopic)
				return nil
			}
			return errors.Wrap(ctx, err, "deserialize record failed")
		}

		// Convert AVRO record to Kafka message
		msg := &sarama.ProducerMessage{
			Topic:     string(targetTopic),
			Key:       sarama.ByteEncoder(record.Key),
			Value:     sarama.ByteEncoder(record.Value),
			Timestamp: time.UnixMilli(record.Timestamp),
		}

		// Convert headers
		for _, header := range record.Headers {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   header.Key,
				Value: header.Value,
			})
		}

		// Send message
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			return errors.Wrapf(
				ctx,
				err,
				"send message failed for partition %d offset %d",
				record.Partition,
				record.Offset,
			)
		}

		messageCount++
		if messageCount%1000 == 0 {
			glog.V(2).
				Infof("restored %d messages (last: partition %d offset %d)", messageCount, partition, offset)
		}
	}
}
