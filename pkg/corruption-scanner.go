// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

// CorruptionScanner scans Kafka topics for CRC corruption and reports corrupt offset ranges.
//
//counterfeiter:generate -o ../mocks/corruption-scanner.go --fake-name CorruptionScanner . CorruptionScanner
type CorruptionScanner interface {
	Scan(ctx context.Context, topic libkafka.Topic) (*ScanResult, error)
}

func NewCorruptionScanner(
	saramaClientProvider libkafka.SaramaClientProvider,
	currentTimeGetter libtime.CurrentTimeGetter,
	partitionFilter int, // -1 = all partitions, >= 0 = specific partition
	startOffsetOverride int64, // -1 = use oldest, >= 0 = start from this offset
	corruptionSkipper CorruptionSkipper,
) CorruptionScanner {
	return &corruptionScanner{
		saramaClientProvider: saramaClientProvider,
		currentTimeGetter:    currentTimeGetter,
		partitionFilter:      partitionFilter,
		startOffsetOverride:  startOffsetOverride,
		corruptionSkipper:    corruptionSkipper,
	}
}

type corruptionScanner struct {
	saramaClientProvider libkafka.SaramaClientProvider
	currentTimeGetter    libtime.CurrentTimeGetter
	partitionFilter      int
	startOffsetOverride  int64
	corruptionSkipper    CorruptionSkipper
}

func (s *corruptionScanner) Scan(
	ctx context.Context,
	topic libkafka.Topic,
) (*ScanResult, error) {
	startTime := s.currentTimeGetter.Now()
	result := &ScanResult{
		Topic:         topic,
		CorruptRanges: make([]CorruptRange, 0),
	}

	saramaClient, err := s.saramaClientProvider.Client(ctx)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "get sarama client failed")
	}
	defer saramaClient.Close()

	// Get all partitions for topic
	partitions, err := saramaClient.Partitions(topic.String())
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "get partitions for topic %s failed", topic)
	}

	// Filter partitions if specific partition requested
	var partitionsToScan []int32
	if s.partitionFilter >= 0 {
		// Verify partition exists
		found := false
		for _, p := range partitions {
			if int(p) == s.partitionFilter {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.Errorf(
				ctx,
				"partition %d not found in topic %s",
				s.partitionFilter,
				topic,
			)
		}
		partitionsToScan = []int32{
			int32(s.partitionFilter),
		} //#nosec G115 -- partition filter validated above
		glog.V(1).
			Infof("Scanning single partition %d (of %d total)", s.partitionFilter, len(partitions))
	} else {
		partitionsToScan = partitions
		glog.V(1).Infof("Scanning all %d partitions", len(partitions))
	}

	// Scan each partition
	for _, partition := range partitionsToScan {
		if err := s.scanPartition(ctx, saramaClient, topic, libkafka.Partition(partition), result); err != nil {
			return nil, err
		}
	}

	result.ScanDurationSec = s.currentTimeGetter.Now().Sub(startTime).Seconds()
	return result, nil
}

// No longer needed - we'll track last message timestamp during scanning

func (s *corruptionScanner) scanPartition(
	ctx context.Context,
	client sarama.Client,
	topic libkafka.Topic,
	partition libkafka.Partition,
	result *ScanResult,
) error {
	// Get offset range
	oldestOffset, err := client.GetOffset(topic.String(), int32(partition), sarama.OffsetOldest)
	if err != nil {
		return errors.Wrapf(
			ctx,
			err,
			"get oldest offset for %s partition %d failed",
			topic,
			partition,
		)
	}

	// Apply start offset override if specified
	if s.startOffsetOverride >= 0 {
		glog.V(1).
			Infof("Overriding start offset from %d to %d", oldestOffset, s.startOffsetOverride)
		oldestOffset = s.startOffsetOverride
	}

	newestOffset, err := client.GetOffset(topic.String(), int32(partition), sarama.OffsetNewest)
	if err != nil {
		return errors.Wrapf(
			ctx,
			err,
			"get newest offset for %s partition %d failed",
			topic,
			partition,
		)
	}

	if oldestOffset == 0 && result.FirstOffset == 0 {
		result.FirstOffset = libkafka.Offset(oldestOffset)
	}
	if newestOffset > int64(result.LastOffset) {
		result.LastOffset = libkafka.Offset(newestOffset)
	}

	glog.V(2).Infof("Scanning %s partition %d: offsets %d to %d (%d messages)",
		topic, partition, oldestOffset, newestOffset-1, newestOffset-oldestOffset)

	// Create partition consumer
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return errors.Wrap(ctx, err, "create consumer failed")
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(
		topic.String(),
		int32(partition),
		oldestOffset,
	)
	if err != nil {
		return errors.Wrapf(ctx, err, "consume partition %d failed", partition)
	}

	// Track if partition consumer needs closing (avoid double-close)
	partitionConsumerClosed := false
	defer func() {
		if !partitionConsumerClosed {
			partitionConsumer.Close()
		}
	}()

	// Track corruption ranges
	var currentRange *CorruptRange
	currentOffset := libkafka.Offset(oldestOffset)
	lastLogTime := s.currentTimeGetter.Now()
	var lastMessageTimestamp int64 // Track last successfully read message timestamp

scanLoop:
	for currentOffset < libkafka.Offset(newestOffset) {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg := <-partitionConsumer.Messages():
			// Successfully read message
			result.TotalMessages++
			currentOffset = libkafka.Offset(msg.Offset + 1)
			lastMessageTimestamp = msg.Timestamp.UnixMilli()

			// End any open corruption range
			if currentRange != nil {
				// Use timestamp from first good message after corruption as end timestamp
				currentRange.EndTimestamp = lastMessageTimestamp
				result.CorruptRanges = append(result.CorruptRanges, *currentRange)
				glog.Infof("Corruption range ended: partition %d, offsets %d-%d (%d messages)",
					currentRange.Partition, currentRange.StartOffset, currentRange.EndOffset,
					currentRange.Count())
				currentRange = nil
			}

			// Progress logging every 10 seconds
			if s.currentTimeGetter.Now().Sub(lastLogTime) > 10*time.Second {
				progress := float64(currentOffset-libkafka.Offset(oldestOffset)) / float64(newestOffset-oldestOffset) * 100
				glog.V(1).Infof("Progress %s partition %d: %.1f%% (%d/%d messages, %d corrupt ranges)",
					topic, partition, progress, result.TotalMessages,
					newestOffset-oldestOffset, len(result.CorruptRanges))
				lastLogTime = s.currentTimeGetter.Now()
			}

		case err := <-partitionConsumer.Errors():
			// Check if this is a corruption error
			if IsCorruptionError(err.Err) {
				// Start or extend corruption range
				if currentRange == nil {
					currentRange = &CorruptRange{
						Partition:      partition,
						StartOffset:    currentOffset,
						EndOffset:      currentOffset,
						StartTimestamp: lastMessageTimestamp, // Use last good message timestamp
						ErrorMessage:   err.Err.Error(),
					}
					glog.Warningf("Corruption detected at offset %d: %v", currentOffset, err.Err)
				} else {
					currentRange.EndOffset = currentOffset
				}

				// Close main consumer before testing offsets to avoid "already being consumed" error
				partitionConsumer.Close()
				partitionConsumerClosed = true

				// Adaptive skip: Find end of corrupt range using exponential search
				skipOffset := s.corruptionSkipper.FindNextHealthyOffset(ctx, consumer, topic, partition, currentOffset, newestOffset)
				if skipOffset < 0 {
					// Couldn't find end, corruption extends to end of topic
					currentRange.EndOffset = libkafka.Offset(newestOffset - 1)
					result.CorruptRanges = append(result.CorruptRanges, *currentRange)
					glog.Warningf("Corruption extends to end of topic: partition %d, offsets %d-%d (%d offsets)",
						currentRange.Partition, currentRange.StartOffset, currentRange.EndOffset,
						currentRange.Count())
					currentRange = nil // Prevent double-add in finalization
					break scanLoop     // Exit outer loop completely
				}

				currentOffset = libkafka.Offset(skipOffset)
				currentRange.EndOffset = currentOffset - 1 // Last known corrupt offset

				// Note: EndTimestamp will be set when we read the first good message after recreating consumer

				// Add completed corruption range to results
				result.CorruptRanges = append(result.CorruptRanges, *currentRange)
				glog.Infof("Corruption range complete: partition %d, offsets %d-%d (%d offsets)",
					currentRange.Partition, currentRange.StartOffset, currentRange.EndOffset,
					currentRange.Count())

				// Reset current range
				currentRange = nil

				glog.Infof("Skipping to offset %d after corruption range", currentOffset)

				// Recreate partition consumer at new offset
				var recreateErr error
				partitionConsumer, recreateErr = consumer.ConsumePartition(topic.String(), int32(partition), int64(currentOffset))
				if recreateErr != nil {
					return errors.Wrapf(ctx, recreateErr, "recreate partition consumer at offset %d failed", currentOffset)
				}
				partitionConsumerClosed = false // New consumer needs closing
				glog.Infof("Successfully recreated partition consumer at offset %d, continuing scan", currentOffset)
			} else {
				// Non-corruption error - fatal
				return errors.Wrapf(ctx, err.Err, "partition consumer error at offset %d", currentOffset)
			}
		}
	}

	// Finalize any open corruption range
	if currentRange != nil {
		result.CorruptRanges = append(result.CorruptRanges, *currentRange)
		glog.Infof("Corruption range (final): partition %d, offsets %d-%d (%d messages)",
			currentRange.Partition, currentRange.StartOffset, currentRange.EndOffset,
			currentRange.Count())
	}

	glog.V(1).Infof("Completed scanning %s partition %d: %d messages, %d corrupt ranges",
		topic, partition, result.TotalMessages, len(result.CorruptRanges))

	return nil
}
