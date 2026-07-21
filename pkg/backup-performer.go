// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
)

// BackupPerformer executes topic backup operations.
//
//counterfeiter:generate -o ../../mocks/backup-performer.go --fake-name BackupPerformer . BackupPerformer
type BackupPerformer interface {
	Backup(
		ctx context.Context,
		backupDate libtime.Date,
		topic libkafka.Topic,
	) (*BackupResult, error)
}

type BackupPerformerFunc func(ctx context.Context, backupDate libtime.Date, topic libkafka.Topic) (*BackupResult, error)

func (b BackupPerformerFunc) Backup(
	ctx context.Context,
	backupDate libtime.Date,
	topic libkafka.Topic,
) (*BackupResult, error) {
	return b(ctx, backupDate, topic)
}

type BackupResult struct {
	MessageCount  int64
	OffsetCount   int64
	Partition     libkafka.Partition
	FirstOffset   libkafka.Offset
	LastOffset    libkafka.Offset
	SkippedRanges []OffsetRange
}
