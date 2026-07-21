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

func NewBackupPerformerTopic(
	topicReader TopicReader,
	opener Opener,
	logSamplerFactory log.SamplerFactory,
) BackupPerformer {
	return &backupPerformerTopic{
		topicReader:       topicReader,
		opener:            opener,
		logSamplerFactory: logSamplerFactory,
	}
}

type backupPerformerTopic struct {
	topicReader       TopicReader
	opener            Opener
	logSamplerFactory log.SamplerFactory
}

func (b *backupPerformerTopic) Backup(
	ctx context.Context,
	backupDate libtime.Date,
	topic libkafka.Topic,
) (*BackupResult, error) {
	writer, err := b.opener.OpenWriter(ctx, backupDate, topic, BackupTypeTopic)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "open writer failed")
	}
	defer writer.Close()

	var messageCount int64

	ch := make(chan avro.BackupTopicRecord, runtime.NumCPU())
	err = run.CancelOnFirstError(
		ctx,
		func(ctx context.Context) error {
			defer close(ch)
			return b.topicReader.Read(ctx, topic, ch)
		},
		func(ctx context.Context) error {
			logSampler := b.logSamplerFactory.Sampler()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case BackupTopicRecord, ok := <-ch:
					if !ok {
						glog.V(2).Infof("read all %d backup entries completed", messageCount)
						return nil
					}
					messageCount++

					if err := BackupTopicRecord.Serialize(writer); err != nil {
						return errors.Wrap(ctx, err, "write failed")
					}

					if logSampler.IsSample() {
						glog.Infof(
							"backup record %s %d %d completed (sample)",
							BackupTopicRecord.Topic,
							BackupTopicRecord.Partition,
							BackupTopicRecord.Offset,
						)
					}
				}
			}
		},
	)
	if err != nil {
		return nil, err
	}
	return &BackupResult{
		MessageCount: messageCount,
	}, nil
}
