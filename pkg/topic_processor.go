// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libsentry "github.com/bborbe/sentry"
	libtime "github.com/bborbe/time"
	"github.com/getsentry/sentry-go"
	"github.com/golang/glog"
)

// TopicProcessor handles the backup workflow for a single topic.
//
//counterfeiter:generate -o ../mocks/topic-processor.go --fake-name TopicProcessor . TopicProcessor
type TopicProcessor interface {
	Process(ctx context.Context, topic libkafka.Topic) error
}

func NewTopicProcessor(
	id int,
	backupDate libtime.Date,
	force bool,
	opener *FileOpener,
	backupManager BackupPerformer,
	sentryClient libsentry.Client,
	currentTimeGetter libtime.CurrentTimeGetter,
	summary BackupSummary,
) TopicProcessor {
	return &topicProcessor{
		id:                id,
		backupDate:        backupDate,
		force:             force,
		opener:            opener,
		backupManager:     backupManager,
		sentryClient:      sentryClient,
		currentTimeGetter: currentTimeGetter,
		summary:           summary,
	}
}

type topicProcessor struct {
	id                int
	backupDate        libtime.Date
	force             bool
	opener            *FileOpener
	backupManager     BackupPerformer
	sentryClient      libsentry.Client
	currentTimeGetter libtime.CurrentTimeGetter
	summary           BackupSummary
}

func (p *topicProcessor) Process(ctx context.Context, topic libkafka.Topic) error {
	// Check if backup already exists
	if !p.force {
		exists, err := p.opener.StatsExists(ctx, p.backupDate, topic)
		if err != nil {
			glog.Warningf(
				"Worker %d: check stats exists for %s failed: %v (proceeding with backup)",
				p.id,
				topic,
				err,
			)
		} else if exists {
			stats, err := p.opener.ReadStats(ctx, p.backupDate, topic)
			if err != nil {
				glog.Warningf("Worker %d: read stats for %s failed: %v (proceeding with backup)", p.id, topic, err)
			} else if stats.Success {
				p.summary.AddSkipped()
				current := p.summary.AddCompleted()
				glog.V(1).Infof("Worker %d [%d/%d] Skipped %s (already exists)", p.id, current, p.summary.Total(), topic)
				return nil
			}
		}
	}

	// Capture stats
	startTime := p.currentTimeGetter.Now()
	stats := BackupStats{
		Topic:     topic.String(),
		StartedAt: startTime,
		Success:   false,
	}

	// Perform backup
	glog.V(2).Infof("backing up %s started", topic)
	result, err := p.backupManager.Backup(ctx, p.backupDate, topic)

	stats.CompletedAt = p.currentTimeGetter.Now()
	stats.DurationSeconds = stats.CompletedAt.Sub(startTime).Seconds()

	if result != nil {
		stats.MessageCount = result.MessageCount
		stats.OffsetCount = result.OffsetCount
		stats.Partition = result.Partition
		stats.FirstOffset = result.FirstOffset
		stats.LastOffset = result.LastOffset
	}

	current := p.summary.AddCompleted()

	if err != nil {
		stats.Error = err.Error()
		p.summary.AddFailed(topic)
		glog.Warningf(
			"Worker %d [%d/%d] Failed %s: %v",
			p.id,
			current,
			p.summary.Total(),
			topic,
			err,
		)

		// Report to Sentry (skip context.Canceled)
		if !errors.Is(err, context.Canceled) {
			p.sentryClient.CaptureException(
				errors.Wrapf(ctx, err, "backup %s failed", topic),
				&sentry.EventHint{Context: ctx},
				nil,
			)
		}

		// Write stats even on failure
		if writeErr := p.opener.WriteStats(ctx, p.backupDate, topic, stats); writeErr != nil {
			return errors.Wrapf(ctx, writeErr, "write stats for %s failed", topic)
		}

		// Only return fatal errors
		if IsBrokenPipeError(err) {
			return errors.Wrapf(ctx, err, "broken pipe for %s", topic)
		}
		// Continue to next topic
		return nil
	}

	// Backup succeeded
	stats.Success = true

	// Write stats.json
	if err := p.opener.WriteStats(ctx, p.backupDate, topic, stats); err != nil {
		return errors.Wrapf(ctx, err, "write stats for %s failed", topic)
	}

	glog.V(1).Infof(
		"Worker %d [%d/%d] Completed %s (%.2fs, %d messages, %d offsets)",
		p.id, current, p.summary.Total(), topic, stats.DurationSeconds,
		stats.MessageCount, stats.OffsetCount,
	)
	return nil
}
