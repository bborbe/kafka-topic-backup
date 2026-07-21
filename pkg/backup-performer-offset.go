// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"runtime"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	"github.com/bborbe/run"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/kafka-topic-backup/avro"
)

func NewBackupPerformerOffset(
	consumerGroupOffsetReader ConsumerGroupOffsetReader,
	opener Opener,
	samplerFactory log.SamplerFactory,
) BackupPerformer {
	return &backupPerformerOffset{
		consumerGroupOffsetReader: consumerGroupOffsetReader,
		opener:                    opener,
		logSampler:                samplerFactory.Sampler(),
	}
}

type backupPerformerOffset struct {
	consumerGroupOffsetReader ConsumerGroupOffsetReader
	opener                    Opener
	logSampler                log.Sampler
}

func (b *backupPerformerOffset) Backup(
	ctx context.Context,
	backupDate libtime.Date,
	topic libkafka.Topic,
) (*BackupResult, error) {
	writer, err := b.opener.OpenWriter(ctx, backupDate, topic, BackupTypeOffset)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "open writer failed")
	}
	defer writer.Close()

	var offsetCount int64
	ch := make(chan avro.BackupTopicConsumerGroupOffset, runtime.NumCPU())
	err = run.CancelOnFirstError(
		ctx,
		func(ctx context.Context) error {
			defer close(ch)
			return b.consumerGroupOffsetReader.Offset(ctx, topic, ch)
		},
		func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case consumerGroupOffset, ok := <-ch:
					if !ok {
						return nil
					}
					offsetCount++
					if err := consumerGroupOffset.Serialize(writer); err != nil {
						return errors.Wrap(ctx, err, "write failed")
					}
					if b.logSampler.IsSample() {
						glog.V(2).
							Infof("backup offset: %+v completed (sample)", consumerGroupOffset)
					}
				}
			}
		},
	)
	if err != nil {
		return nil, err
	}
	return &BackupResult{
		OffsetCount: offsetCount,
	}, nil
}
