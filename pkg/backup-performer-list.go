// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
)

type BackupPerformerList []BackupPerformer

func (b BackupPerformerList) Backup(
	ctx context.Context,
	backupDate libtime.Date,
	topic libkafka.Topic,
) (*BackupResult, error) {
	result := &BackupResult{}
	for _, backupPerformer := range b {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			performerResult, err := backupPerformer.Backup(ctx, backupDate, topic)
			if err != nil {
				return nil, errors.Wrapf(
					ctx,
					err,
					"run backup performer %T failed",
					backupPerformer,
				)
			}
			if performerResult != nil {
				result.MessageCount += performerResult.MessageCount
				result.OffsetCount += performerResult.OffsetCount
			}
		}
	}
	return result, nil
}
