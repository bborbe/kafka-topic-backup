// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"io"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/kafka-topic-backup/avro"
)

func NewConsumerGroupOffsetRestorer(
	saramaClient libkafka.SaramaClient,
	opener Opener,
) *ConsumerGroupOffsetRestorer {
	return &ConsumerGroupOffsetRestorer{
		saramaClient: saramaClient,
		opener:       opener,
	}
}

type ConsumerGroupOffsetRestorer struct {
	saramaClient libkafka.SaramaClient
	opener       Opener
}

func (c *ConsumerGroupOffsetRestorer) Restore(
	ctx context.Context,
	backupDate libtime.Date,
	topic libkafka.Topic,
) error {
	// Open offset.avro file
	reader, err := c.opener.OpenReader(ctx, backupDate, topic, BackupTypeOffset)
	if err != nil {
		return errors.Wrap(ctx, err, "open offset backup failed")
	}
	defer reader.Close()

	// Create OffsetManager to commit offsets
	offsetManager, err := sarama.NewOffsetManagerFromClient("", c.saramaClient)
	if err != nil {
		return errors.Wrap(ctx, err, "create offset manager failed")
	}
	defer offsetManager.Close()

	// Read and restore offsets
	offsetCount := 0
	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx, ctx.Err(), "context cancelled during offset restore")
		default:
		}

		record, err := avro.DeserializeBackupTopicConsumerGroupOffset(reader)
		if err != nil {
			if err == io.EOF {
				glog.V(2).Infof("restored %d consumer group offsets for %s", offsetCount, topic)
				return nil
			}
			return errors.Wrap(ctx, err, "deserialize offset record failed")
		}

		// Get or create partition offset manager
		partitionManager, err := offsetManager.ManagePartition(record.Topic, record.Partition)
		if err != nil {
			glog.Warningf(
				"manage partition failed for consumer group %s topic %s partition %d: %v",
				record.ConsumerGroup,
				record.Topic,
				record.Partition,
				err,
			)
			continue
		}

		// Mark offset (MarkOffset just updates in-memory, the offset will be committed when offset manager is closed)
		partitionManager.MarkOffset(record.Offset, "")
		partitionManager.Close()

		offsetCount++
	}
}
