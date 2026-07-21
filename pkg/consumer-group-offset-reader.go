// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	"github.com/golang/glog"

	"github.com/bborbe/kafka-topic-backup/avro"
)

type ConsumerGroupOffsetReader interface {
	Offset(
		ctx context.Context,
		topic libkafka.Topic,
		ch chan<- avro.BackupTopicConsumerGroupOffset,
	) error
}

func NewConsumerGroupOffsetReader(
	saramaClientProvider libkafka.SaramaClientProvider,
	samplerFactory log.SamplerFactory,
) ConsumerGroupOffsetReader {
	return &consumerGroupOffsetReader{
		saramaClientProvider: saramaClientProvider,
		logSampler:           samplerFactory.Sampler(),
	}
}

type consumerGroupOffsetReader struct {
	saramaClientProvider libkafka.SaramaClientProvider
	logSampler           log.Sampler
}

func (b *consumerGroupOffsetReader) Offset(
	ctx context.Context,
	topic libkafka.Topic,
	ch chan<- avro.BackupTopicConsumerGroupOffset,
) error {
	glog.V(2).Infof("read all offsets for topic %s started", topic)

	// Get client from pool (health-checked, released on Close)
	saramaClient, err := b.saramaClientProvider.Client(ctx)
	if err != nil {
		return errors.Wrap(ctx, err, "get sarama client failed")
	}
	defer saramaClient.Close()

	partitions, err := saramaClient.Partitions(topic.String())
	if err != nil {
		return errors.Wrap(ctx, err, "get partitions failed")
	}

	clusterAdmin, err := sarama.NewClusterAdminFromClient(saramaClient)
	if err != nil {
		return errors.Wrap(ctx, err, "create clusterAdmin failed")
	}

	groups, err := clusterAdmin.ListConsumerGroups()
	if err != nil {
		return errors.Wrap(ctx, err, "list consumer groups failed")
	}

	for consumerGroup := range groups {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := b.getOffsets(ctx, saramaClient, topic, consumerGroup, partitions, ch); err != nil {
				return errors.Wrap(ctx, err, "get offsets failed")
			}
		}
	}
	if b.logSampler.IsSample() {
		glog.V(2).Infof("read all offsets for topic %s completed (sample)", topic)
	}
	return nil
}

func (b *consumerGroupOffsetReader) getOffsets(
	ctx context.Context,
	saramaClient libkafka.SaramaClient,
	topic libkafka.Topic,
	consumerGroup string,
	partitions []int32,
	ch chan<- avro.BackupTopicConsumerGroupOffset,
) error {
	glog.V(3).Infof("handle consumerGroup %s", consumerGroup)

	offsetManager, err := sarama.NewOffsetManagerFromClient(consumerGroup, saramaClient)
	if err != nil {
		return errors.Wrap(ctx, err, "create offset manager failed")
	}
	defer offsetManager.Close()

	for _, partition := range partitions {
		partitionOffsetManager, err := offsetManager.ManagePartition(topic.String(), partition)
		if err != nil {
			return errors.Wrap(ctx, err, "manage partitions failed")
		}
		offset, _ := partitionOffsetManager.NextOffset()
		if offset == -2 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- avro.BackupTopicConsumerGroupOffset{
			Topic:         topic.String(),
			ConsumerGroup: consumerGroup,
			Partition:     partition,
			Offset:        offset,
		}:
			glog.V(2).Infof(
				"topic: %s group: %s partition: %d offset: %d",
				topic, consumerGroup, partition, offset,
			)
		}
	}
	return nil
}
