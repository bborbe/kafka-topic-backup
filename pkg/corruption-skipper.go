// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	"github.com/golang/glog"
)

// CorruptionSkipper finds the next healthy offset after a corrupt range.
//
//counterfeiter:generate -o ../mocks/corruption-skipper.go --fake-name CorruptionSkipper . CorruptionSkipper
type CorruptionSkipper interface {
	// FindNextHealthyOffset returns the first good offset after corruption, or -1 if not found.
	FindNextHealthyOffset(
		ctx context.Context,
		consumer sarama.Consumer,
		topic libkafka.Topic,
		partition libkafka.Partition,
		corruptOffset libkafka.Offset,
		maxOffset int64,
	) int64
}

func NewCorruptionSkipper(logSamplerFactory log.SamplerFactory) CorruptionSkipper {
	return &corruptionSkipper{
		logSamplerFactory: logSamplerFactory,
	}
}

type corruptionSkipper struct {
	logSamplerFactory log.SamplerFactory
}

func (s *corruptionSkipper) FindNextHealthyOffset(
	ctx context.Context,
	consumer sarama.Consumer,
	topic libkafka.Topic,
	partition libkafka.Partition,
	corruptOffset libkafka.Offset,
	maxOffset int64,
) int64 {
	// Try exponentially increasing jumps: +10, +100, +1000, +10000
	jumps := []int64{10, 100, 1000, 10000, 100000}
	testOffset := int64(corruptOffset)
	logSampler := s.logSamplerFactory.Sampler()

	glog.V(1).Infof("Searching for end of corruption starting at offset %d", corruptOffset)

	// Exponential search: find upper bound where corruption ends
	for _, jump := range jumps {
		testOffset = int64(corruptOffset) + jump
		if testOffset >= maxOffset {
			glog.V(1).Infof("Jump to %d exceeds max offset %d", testOffset, maxOffset)
			return -1 // Corruption extends to end of topic
		}

		if logSampler.IsSample() {
			glog.Infof("Testing offset %d (jump +%d) (sample)", testOffset, jump)
		}
		if s.isOffsetGood(ctx, consumer, topic, partition, testOffset) {
			glog.V(1).Infof("Found good offset at %d, corruption ends before this", testOffset)
			// Binary search between corruptOffset and testOffset to find exact end
			return s.binarySearchEndOfCorruption(
				ctx,
				consumer,
				topic,
				partition,
				int64(corruptOffset),
				testOffset,
				logSampler,
			)
		}
		if logSampler.IsSample() {
			glog.Infof(
				"Offset %d still corrupt or compacted, trying larger jump (sample)",
				testOffset,
			)
		}
	}

	// All exponential jumps failed - might be corruption OR massive compaction gap
	// Test near end of topic to distinguish between the two
	nearEndOffset := maxOffset - 10000 // Try 10k offsets before end
	if nearEndOffset > testOffset {
		glog.V(1).Infof("All jumps failed, testing near end of topic at offset %d", nearEndOffset)
		if s.isOffsetGood(ctx, consumer, topic, partition, nearEndOffset) {
			// Can read near end - there's a massive compaction gap
			// Find where corruption actually ends before the compaction gap
			glog.Infof(
				"Massive compaction gap detected between %d and %d, finding actual end of corruption",
				corruptOffset,
				nearEndOffset,
			)

			// Try to find end of corruption within a reasonable range after start
			// Don't search the entire gap - corruption likely ends soon after it starts
			searchLimit := int64(
				corruptOffset,
			) + 1000 // Search up to 1000 offsets after corruption start
			if searchLimit > nearEndOffset {
				searchLimit = nearEndOffset
			}

			// Try to find actual corruption end within limited range
			actualEnd := s.findActualCorruptionEnd(
				ctx,
				consumer,
				topic,
				partition,
				int64(corruptOffset),
				searchLimit,
				logSampler,
			)

			if actualEnd >= searchLimit {
				// Corruption extends through entire search range
				// Can't determine actual end - must jump to near-end to skip compaction gap
				glog.Infof(
					"Corruption extends beyond %d (searched 1000 offsets), jumping to %d to skip compaction gap",
					searchLimit,
					nearEndOffset,
				)
				// Return nearEndOffset to jump over the compaction gap
				// Scanner will mark corruption range as corruptOffset to nearEndOffset-1
				return nearEndOffset
			}

			// Found actual end within search range
			glog.Infof(
				"Corruption ends at offset %d, continuing scan from %d",
				actualEnd-1,
				actualEnd,
			)
			return actualEnd
		}
	}

	// If all jumps still corrupt/timeout, corruption extends to end
	glog.Warningf("Corruption extends beyond offset %d to end of topic", testOffset)
	return -1
}

// findActualCorruptionEnd finds where corruption ends before a compaction gap.
// Returns first good offset or compaction gap start (whichever comes first).
func (s *corruptionSkipper) findActualCorruptionEnd(
	ctx context.Context,
	consumer sarama.Consumer,
	topic libkafka.Topic,
	partition libkafka.Partition,
	corruptOffset int64,
	searchLimit int64,
	logSampler log.Sampler,
) int64 {
	glog.V(1).Infof(
		"Searching for actual corruption end between %d and %d (before compaction gap)",
		corruptOffset,
		searchLimit,
	)

	// Try sequential offsets after corruption start to find where it ends
	for testOffset := corruptOffset + 1; testOffset <= searchLimit; testOffset++ {
		if logSampler.IsSample() {
			glog.Infof("Testing offset %d for corruption end (sample)", testOffset)
		}

		if s.isOffsetGood(ctx, consumer, topic, partition, testOffset) {
			glog.V(1).
				Infof("Corruption ends at %d, next good offset is %d", testOffset-1, testOffset)
			return testOffset
		}
	}

	// Corruption extends through entire search range - return limit
	glog.V(1).Infof("Corruption extends to search limit %d", searchLimit)
	return searchLimit
}

// binarySearchEndOfCorruption finds exact end of corruption range.
// Returns first good offset after corruption.
func (s *corruptionSkipper) binarySearchEndOfCorruption(
	ctx context.Context,
	consumer sarama.Consumer,
	topic libkafka.Topic,
	partition libkafka.Partition,
	corruptOffset int64,
	goodOffset int64,
	logSampler log.Sampler,
) int64 {
	glog.V(1).
		Infof("Binary searching between corrupt offset %d and good offset %d", corruptOffset, goodOffset)

	for corruptOffset+1 < goodOffset {
		mid := (corruptOffset + goodOffset) / 2
		if logSampler.IsSample() {
			glog.Infof("Testing mid offset %d (sample)", mid)
		}

		if s.isOffsetGood(ctx, consumer, topic, partition, mid) {
			goodOffset = mid
		} else {
			corruptOffset = mid
		}
	}

	glog.V(1).
		Infof("Corruption ends at offset %d, first good offset is %d", corruptOffset, goodOffset)
	return goodOffset
}

// isOffsetGood tests if we can read a message starting from this offset without CRC error.
// For compacted topics, Kafka may return a message at a higher offset - this is still considered "good"
// since it means no corruption from this point forward.
func (s *corruptionSkipper) isOffsetGood(
	ctx context.Context,
	consumer sarama.Consumer,
	topic libkafka.Topic,
	partition libkafka.Partition,
	offset int64,
) bool {
	pc, err := consumer.ConsumePartition(topic.String(), int32(partition), offset)
	if err != nil {
		glog.V(3).Infof("Failed to create consumer at offset %d: %v", offset, err)
		return false
	}
	defer pc.Close()

	// Try to read one message with timeout
	timeout := time.NewTimer(5 * time.Second) // Longer timeout for compacted topics with large gaps
	defer timeout.Stop()

	select {
	case <-ctx.Done():
		return false
	case msg := <-pc.Messages():
		// Got a message - offset may be higher due to compaction, but that's fine
		// It means we can successfully read from this position onwards
		if msg.Offset > offset {
			glog.V(3).Infof("Offset %d compacted, next available is %d (gap of %d)",
				offset, msg.Offset, msg.Offset-offset)
		}
		glog.V(4).Infof("Successfully read message at offset %d", msg.Offset)
		return true // Can read from here, no corruption
	case err := <-pc.Errors():
		if IsCorruptionError(err.Err) {
			glog.V(3).Infof("Offset %d is corrupt: %v", offset, err.Err)
			return false
		}
		glog.V(3).Infof("Offset %d has error (non-corruption): %v", offset, err.Err)
		return false
	case <-timeout.C:
		glog.V(3).
			Infof("Timeout reading from offset %d (end of topic or massive compaction gap)", offset)
		return false // Likely at end of topic
	}
}
