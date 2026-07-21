// Copyright (c) 2025 Benjamin Borbe All rights reserved.
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

// ReadResult contains the result of reading a partition.
type ReadResult struct {
	SkippedRanges []OffsetRange
}

// PartitionReader reads messages from a single Kafka partition for backup.
//
//counterfeiter:generate -o ../mocks/partition-reader.go --fake-name PartitionReader . PartitionReader
type PartitionReader interface {
	// Read reads messages from a partition. If startOffset > 0, reading starts from that offset;
	// otherwise it starts from the oldest available offset. If skipCorrupt is true, CRC errors
	// are skipped and the skipped ranges are returned in the result.
	Read(
		ctx context.Context,
		topic libkafka.Topic,
		partition libkafka.Partition,
		startOffset libkafka.Offset,
		skipCorrupt bool,
		ch chan<- avro.BackupTopicRecord,
	) (*ReadResult, error)
}

func NewPartitionReader(
	saramaClientProvider libkafka.SaramaClientProvider,
	converter ConsumerMessageConverter,
	logSamplerFactory log.SamplerFactory,
	corruptionSkipper CorruptionSkipper,
) PartitionReader {
	return &partitionReader{
		saramaClientProvider: saramaClientProvider,
		converter:            converter,
		logSamplerFactory:    logSamplerFactory,
		corruptionSkipper:    corruptionSkipper,
	}
}

type partitionReader struct {
	saramaClientProvider libkafka.SaramaClientProvider
	converter            ConsumerMessageConverter
	logSamplerFactory    log.SamplerFactory
	corruptionSkipper    CorruptionSkipper
}

func (p *partitionReader) Read(
	ctx context.Context,
	topic libkafka.Topic,
	partition libkafka.Partition,
	startOffset libkafka.Offset,
	skipCorrupt bool,
	ch chan<- avro.BackupTopicRecord,
) (*ReadResult, error) {
	glog.V(2).
		Infof("read partition %s:%d started (startOffset=%d, skipCorrupt=%v)", topic, partition, startOffset, skipCorrupt)
	result, err := p.read(ctx, topic, partition, startOffset, skipCorrupt, ch)
	if err != nil {
		glog.V(1).Infof("read partition %s:%d failed: %v", topic, partition, err)
		return nil, errors.Wrapf(ctx, err, "read partition %s:%d failed", topic, partition)
	}
	glog.V(2).Infof("read partition %s:%d completed", topic, partition)
	return result, nil
}

func (p *partitionReader) read(
	ctx context.Context,
	topic libkafka.Topic,
	partition libkafka.Partition,
	startOffset libkafka.Offset,
	skipCorrupt bool,
	ch chan<- avro.BackupTopicRecord,
) (*ReadResult, error) {
	result := &ReadResult{}

	saramaClient, err := p.saramaClientProvider.Client(ctx)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "get sarama client failed")
	}
	defer saramaClient.Close()

	// Get offset range for partition
	oldestOffset, err := saramaClient.GetOffset(
		topic.String(),
		int32(partition),
		sarama.OffsetOldest,
	)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "get oldest offset for %s:%d failed", topic, partition)
	}

	newestOffset, err := saramaClient.GetOffset(
		topic.String(),
		int32(partition),
		sarama.OffsetNewest,
	)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "get newest offset for %s:%d failed", topic, partition)
	}

	// Determine actual start offset
	actualStartOffset := oldestOffset
	if startOffset > 0 && int64(startOffset) > oldestOffset {
		actualStartOffset = int64(startOffset)
	}

	// Check if there's anything to read
	if actualStartOffset >= newestOffset {
		glog.V(2).
			Infof("partition %s:%d is up to date (start=%d, newest=%d)", topic, partition, actualStartOffset, newestOffset)
		return result, nil
	}

	glog.V(2).
		Infof("reading partition %s:%d from offset %d to %d", topic, partition, actualStartOffset, newestOffset)

	// Create consumer
	consumer, err := sarama.NewConsumerFromClient(saramaClient)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "create consumer failed")
	}
	defer consumer.Close()

	// Consume partition
	partitionConsumer, err := consumer.ConsumePartition(
		topic.String(),
		int32(partition),
		actualStartOffset,
	)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "consume partition %s:%d failed", topic, partition)
	}

	// Track if partition consumer needs closing
	partitionConsumerClosed := false
	defer func() {
		if !partitionConsumerClosed {
			partitionConsumer.Close()
		}
	}()

	logSampler := p.logSamplerFactory.Sampler()
	var messageCount int64
	currentOffset := libkafka.Offset(actualStartOffset)

readLoop:
	for currentOffset < libkafka.Offset(newestOffset) {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case consumerErr := <-partitionConsumer.Errors():
			if skipCorrupt && IsCorruptionError(consumerErr.Err) {
				glog.Warningf("corruption detected at %s:%d offset %d: %v", topic, partition, currentOffset, consumerErr.Err)

				// Close consumer before probing offsets
				partitionConsumer.Close()
				partitionConsumerClosed = true

				// Find next healthy offset
				skipOffset := p.corruptionSkipper.FindNextHealthyOffset(ctx, consumer, topic, partition, currentOffset, newestOffset)
				if skipOffset < 0 {
					// Corruption extends to end of topic
					result.SkippedRanges = append(result.SkippedRanges, OffsetRange{
						Start: int64(currentOffset),
						End:   newestOffset - 1,
					})
					glog.Warningf("corruption extends to end of %s:%d from offset %d", topic, partition, currentOffset)
					break readLoop
				}

				// Record skipped range
				result.SkippedRanges = append(result.SkippedRanges, OffsetRange{
					Start: int64(currentOffset),
					End:   skipOffset - 1,
				})
				glog.Infof("skipping corrupt range %s:%d offsets %d-%d", topic, partition, currentOffset, skipOffset-1)

				// Recreate partition consumer at healthy offset
				currentOffset = libkafka.Offset(skipOffset)
				partitionConsumer, err = consumer.ConsumePartition(topic.String(), int32(partition), skipOffset)
				if err != nil {
					return result, errors.Wrapf(ctx, err, "recreate partition consumer at offset %d failed", skipOffset)
				}
				partitionConsumerClosed = false
				continue
			}
			return result, errors.Wrapf(ctx, consumerErr.Err, "partition consumer error for %s:%d", topic, partition)

		case msg := <-partitionConsumer.Messages():
			messageCount++
			currentOffset = libkafka.Offset(msg.Offset + 1)

			select {
			case <-ctx.Done():
				return result, ctx.Err()
			case ch <- p.converter.Convert(msg):
			}

			if logSampler.IsSample() {
				glog.V(2).Infof(
					"read partition %s:%d offset %d (sample, count=%d)",
					topic, partition, msg.Offset, messageCount,
				)
			}

			// Check if we've reached the highwater mark
			if msg.Offset+1 >= newestOffset {
				glog.V(2).Infof(
					"reached highwater mark for %s:%d at offset %d (%d messages)",
					topic, partition, msg.Offset, messageCount,
				)
				return result, nil
			}
		}
	}

	return result, nil
}
