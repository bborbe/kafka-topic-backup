// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/bborbe/errors"
	libsentry "github.com/bborbe/sentry"
	libtime "github.com/bborbe/time"
	"github.com/getsentry/sentry-go"
	"github.com/golang/glog"
)

// PartitionProcessor handles the backup workflow for a single partition.
//
//counterfeiter:generate -o ../mocks/partition-processor.go --fake-name PartitionProcessor . PartitionProcessor
type PartitionProcessor interface {
	Process(ctx context.Context, task PartitionTask) error
}

func NewPartitionProcessor(
	id int,
	opener *FileOpener,
	backupManager PartitionBackupPerformer,
	sentryClient libsentry.Client,
	currentTimeGetter libtime.CurrentTimeGetter,
	summary BackupSummary,
) PartitionProcessor {
	return &partitionProcessor{
		id:                id,
		opener:            opener,
		backupManager:     backupManager,
		sentryClient:      sentryClient,
		currentTimeGetter: currentTimeGetter,
		summary:           summary,
	}
}

type partitionProcessor struct {
	id                int
	opener            *FileOpener
	backupManager     PartitionBackupPerformer
	sentryClient      libsentry.Client
	currentTimeGetter libtime.CurrentTimeGetter
	summary           BackupSummary
}

func (p *partitionProcessor) Process(ctx context.Context, task PartitionTask) error {
	topic := task.Topic
	partition := task.Partition

	// Capture stats
	startTime := p.currentTimeGetter.Now()
	stats := BackupStats{
		Topic:     topic.String(),
		Partition: partition,
		StartedAt: startTime,
		Success:   false,
	}

	// Perform backup (resume is handled internally by the performer)
	glog.V(2).Infof("backing up %s:%d started", topic, partition)
	result, err := p.backupManager.Backup(ctx, topic, partition)

	stats.CompletedAt = p.currentTimeGetter.Now()
	stats.DurationSeconds = stats.CompletedAt.Sub(startTime).Seconds()

	if result != nil {
		stats.MessageCount = result.MessageCount
		stats.OffsetCount = result.OffsetCount
		stats.FirstOffset = result.FirstOffset
		stats.LastOffset = result.LastOffset
		stats.SkippedRanges = result.SkippedRanges
	}

	current := p.summary.AddCompleted()

	if err != nil {
		stats.Error = err.Error()
		p.summary.AddFailed(topic)
		glog.Warningf(
			"Worker %d [%d/%d] Failed %s:%d: %v",
			p.id,
			current,
			p.summary.Total(),
			topic,
			partition,
			err,
		)

		// Report to Sentry (skip context.Canceled)
		if !errors.Is(err, context.Canceled) {
			p.sentryClient.CaptureException(
				errors.Wrapf(ctx, err, "backup %s:%d failed", topic, partition),
				&sentry.EventHint{Context: ctx},
				nil,
			)
		}

		// Write stats even on failure
		if writeErr := p.opener.WriteStatsPartition(ctx, topic, partition, stats); writeErr != nil {
			return errors.Wrapf(ctx, writeErr, "write stats for %s:%d failed", topic, partition)
		}

		// Only return fatal errors
		if IsBrokenPipeError(err) {
			return errors.Wrapf(ctx, err, "broken pipe for %s:%d", topic, partition)
		}
		// Continue to next partition
		return nil
	}

	// Backup succeeded
	stats.Success = true

	// Write stats.json
	if err := p.opener.WriteStatsPartition(ctx, topic, partition, stats); err != nil {
		return errors.Wrapf(ctx, err, "write stats for %s:%d failed", topic, partition)
	}

	if len(stats.SkippedRanges) > 0 {
		// Record CRC errors in summary
		p.summary.AddCRCErrors(topic, partition, len(stats.SkippedRanges))

		glog.V(1).Infof(
			"Worker %d [%d/%d] Completed %s:%d (%.2fs, %d messages, offsets %d-%d, skipped %d ranges)",
			p.id, current, p.summary.Total(), topic, partition, stats.DurationSeconds,
			stats.MessageCount, stats.FirstOffset, stats.LastOffset, len(stats.SkippedRanges),
		)
	} else {
		glog.V(1).Infof(
			"Worker %d [%d/%d] Completed %s:%d (%.2fs, %d messages, offsets %d-%d)",
			p.id, current, p.summary.Total(), topic, partition, stats.DurationSeconds,
			stats.MessageCount, stats.FirstOffset, stats.LastOffset,
		)
	}
	return nil
}
