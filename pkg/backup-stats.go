// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"time"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
)

// OffsetRange represents a range of offsets.
type OffsetRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type BackupStats struct {
	Topic           string             `json:"topic"`
	Partition       libkafka.Partition `json:"partition,omitempty"`
	StartedAt       time.Time          `json:"started_at"`
	CompletedAt     time.Time          `json:"completed_at"`
	DurationSeconds float64            `json:"duration_seconds"`
	MessageCount    int64              `json:"message_count"`
	OffsetCount     int64              `json:"offset_count"`
	FirstOffset     libkafka.Offset    `json:"first_offset,omitempty"`
	LastOffset      libkafka.Offset    `json:"last_offset,omitempty"`
	SkippedRanges   []OffsetRange      `json:"skipped_ranges,omitempty"`
	Success         bool               `json:"success"`
	Error           string             `json:"error,omitempty"`
}

func (b *FileOpener) WriteStats(
	ctx context.Context,
	date libtime.Date,
	topic libkafka.Topic,
	stats BackupStats,
) error {
	statsFile := path.Join(b.toDir(date, topic), "stats.json")
	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return errors.Wrap(ctx, err, "marshal stats failed")
	}
	if err := os.WriteFile(statsFile, data, 0600); err != nil {
		return errors.Wrap(ctx, err, "write stats failed")
	}
	return nil
}

func (b *FileOpener) ReadStats(
	ctx context.Context,
	date libtime.Date,
	topic libkafka.Topic,
) (*BackupStats, error) {
	statsFile := path.Join(b.toDir(date, topic), "stats.json")
	data, err := os.ReadFile(statsFile) // #nosec G304 -- path constructed from trusted rootDir
	if err != nil {
		return nil, errors.Wrap(ctx, err, "read stats failed")
	}
	var stats BackupStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, errors.Wrap(ctx, err, "unmarshal stats failed")
	}
	return &stats, nil
}

func (b *FileOpener) StatsExists(
	ctx context.Context,
	date libtime.Date,
	topic libkafka.Topic,
) (bool, error) {
	statsFile := path.Join(b.toDir(date, topic), "stats.json")
	_, err := os.Stat(statsFile)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(ctx, err, "check stats failed")
	}
	return true, nil
}

// WriteStatsPartition writes stats for a specific partition.
func (b *FileOpener) WriteStatsPartition(
	ctx context.Context,
	topic libkafka.Topic,
	partition libkafka.Partition,
	stats BackupStats,
) error {
	statsFile := path.Join(b.toDirPartition(topic, partition), "stats.json")
	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return errors.Wrap(ctx, err, "marshal stats failed")
	}
	if err := os.WriteFile(statsFile, data, 0600); err != nil {
		return errors.Wrap(ctx, err, "write stats failed")
	}
	return nil
}

// ReadStatsPartition reads stats for a specific partition.
func (b *FileOpener) ReadStatsPartition(
	ctx context.Context,
	topic libkafka.Topic,
	partition libkafka.Partition,
) (*BackupStats, error) {
	statsFile := path.Join(b.toDirPartition(topic, partition), "stats.json")
	data, err := os.ReadFile(statsFile) // #nosec G304 -- path constructed from trusted rootDir
	if err != nil {
		return nil, errors.Wrap(ctx, err, "read stats failed")
	}
	var stats BackupStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, errors.Wrap(ctx, err, "unmarshal stats failed")
	}
	return &stats, nil
}

// StatsExistsPartition checks if stats exist for a specific partition.
func (b *FileOpener) StatsExistsPartition(
	ctx context.Context,
	topic libkafka.Topic,
	partition libkafka.Partition,
) (bool, error) {
	statsFile := path.Join(b.toDirPartition(topic, partition), "stats.json")
	_, err := os.Stat(statsFile)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(ctx, err, "check stats failed")
	}
	return true, nil
}
