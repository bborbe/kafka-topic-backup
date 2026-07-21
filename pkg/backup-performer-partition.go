// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"os"
	"runtime"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	"github.com/bborbe/run"
	"github.com/golang/glog"

	"github.com/bborbe/kafka-topic-backup/avro"
)

// PartitionBackupPerformer executes backup for a single partition.
//
//counterfeiter:generate -o ../mocks/partition-backup-performer.go --fake-name PartitionBackupPerformer . PartitionBackupPerformer
type PartitionBackupPerformer interface {
	Backup(
		ctx context.Context,
		topic libkafka.Topic,
		partition libkafka.Partition,
	) (*BackupResult, error)
}

func NewBackupPerformerPartition(
	partitionReader PartitionReader,
	opener *FileOpener,
	logSamplerFactory log.SamplerFactory,
	skipCorrupt bool,
) PartitionBackupPerformer {
	return &backupPerformerPartition{
		partitionReader:   partitionReader,
		opener:            opener,
		logSamplerFactory: logSamplerFactory,
		skipCorrupt:       skipCorrupt,
	}
}

type backupPerformerPartition struct {
	partitionReader   PartitionReader
	opener            *FileOpener
	logSamplerFactory log.SamplerFactory
	skipCorrupt       bool
}

func (b *backupPerformerPartition) Backup(
	ctx context.Context,
	topic libkafka.Topic,
	partition libkafka.Partition,
) (*BackupResult, error) {
	// Check for existing backup to determine resume offset
	var startOffset libkafka.Offset
	var existingFirstOffset libkafka.Offset = -1
	var existingMessageCount int64
	appendMode := false

	existingStats, err := b.opener.ReadStatsPartition(ctx, topic, partition)
	if err == nil && existingStats.Success {
		// Resume from last offset + 1
		startOffset = existingStats.LastOffset + 1
		existingFirstOffset = existingStats.FirstOffset
		existingMessageCount = existingStats.MessageCount
		appendMode = true
		glog.V(2).Infof("resuming backup for %s:%d from offset %d", topic, partition, startOffset)
	} else if !os.IsNotExist(errors.Unwrap(err)) && err != nil {
		glog.V(2).Infof("no existing backup for %s:%d, starting fresh", topic, partition)
	}

	writer, err := b.opener.OpenWriterPartition(ctx, topic, partition, BackupTypeTopic, appendMode)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "open writer failed")
	}
	defer writer.Close()

	var messageCount int64
	var firstOffset libkafka.Offset = -1
	var lastOffset libkafka.Offset = -1
	var readResult *ReadResult

	ch := make(chan avro.BackupTopicRecord, runtime.NumCPU())
	err = run.CancelOnFirstError(
		ctx,
		func(ctx context.Context) error {
			defer close(ch)
			var readErr error
			readResult, readErr = b.partitionReader.Read(
				ctx,
				topic,
				partition,
				startOffset,
				b.skipCorrupt,
				ch,
			)
			return readErr
		},
		func(ctx context.Context) error {
			logSampler := b.logSamplerFactory.Sampler()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case record, ok := <-ch:
					if !ok {
						glog.V(2).
							Infof("read all %d backup entries for %s:%d completed", messageCount, topic, partition)
						return nil
					}
					messageCount++

					// Track first and last offsets
					offset := libkafka.Offset(record.Offset)
					if firstOffset < 0 {
						firstOffset = offset
					}
					lastOffset = offset

					if err := record.Serialize(writer); err != nil {
						return errors.Wrap(ctx, err, "write failed")
					}

					if logSampler.IsSample() {
						glog.Infof(
							"backup record %s %d %d completed (sample)",
							record.Topic,
							record.Partition,
							record.Offset,
						)
					}
				}
			}
		},
	)
	if err != nil {
		return nil, err
	}

	// Collect skipped ranges from reader result
	var skippedRanges []OffsetRange
	if readResult != nil {
		skippedRanges = readResult.SkippedRanges
	}

	// Determine final first offset (preserve existing on resume)
	finalFirstOffset := firstOffset
	if appendMode && existingFirstOffset >= 0 {
		finalFirstOffset = existingFirstOffset
	}

	// Determine final last offset (use new if we read anything)
	finalLastOffset := lastOffset
	if lastOffset < 0 && appendMode {
		finalLastOffset = existingStats.LastOffset
	}

	return &BackupResult{
		MessageCount:  existingMessageCount + messageCount,
		Partition:     partition,
		FirstOffset:   finalFirstOffset,
		LastOffset:    finalLastOffset,
		SkippedRanges: skippedRanges,
	}, nil
}
