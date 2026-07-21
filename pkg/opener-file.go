// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

func NewFileOpener(rootDir string) *FileOpener {
	return &FileOpener{
		rootDir: rootDir,
	}
}

type FileOpener struct {
	rootDir string
}

func (b *FileOpener) List(ctx context.Context) (Dates, error) {
	entries, err := os.ReadDir(b.rootDir)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "read dir failed")
	}
	var dates Dates
	for _, entry := range entries {
		date, err := ParseDate(ctx, entry.Name())
		if err != nil {
			continue
		}
		dates = append(dates, *date)
	}
	return dates, nil
}

func (b *FileOpener) OpenReader(
	ctx context.Context,
	date libtime.Date,
	topic libkafka.Topic,
	backupType BackupType,
) (io.ReadCloser, error) {
	file, err := os.OpenFile(b.toFile(date, topic, backupType), os.O_RDONLY, 0600)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "open file failed")
	}
	return file, nil
}

func (b *FileOpener) OpenWriter(
	ctx context.Context,
	date libtime.Date,
	topic libkafka.Topic,
	backupType BackupType,
) (io.WriteCloser, error) {
	if err := b.createBucket(ctx, date, topic); err != nil {
		return nil, errors.Wrap(ctx, err, "create backup dir")
	}

	file, err := os.OpenFile(
		b.toFile(date, topic, backupType),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0600,
	)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "open file failed")
	}
	return file, nil
}

func (b *FileOpener) toFile(date libtime.Date, topic libkafka.Topic, backupType BackupType) string {
	return path.Join(
		b.rootDir,
		date.Format(DateLayout),
		topic.String(),
		fmt.Sprintf("%s.avro", backupType),
	)
}

func (b *FileOpener) toDir(date libtime.Date, topic libkafka.Topic) string {
	return path.Join(b.rootDir, date.Format(DateLayout), topic.String())
}

func (b *FileOpener) toDirPartition(topic libkafka.Topic, partition libkafka.Partition) string {
	return path.Join(b.rootDir, topic.String(), fmt.Sprintf("%d", partition))
}

func (b *FileOpener) toFilePartition(
	topic libkafka.Topic,
	partition libkafka.Partition,
	backupType BackupType,
) string {
	return path.Join(
		b.rootDir,
		topic.String(),
		fmt.Sprintf("%d", partition),
		fmt.Sprintf("%s.avro", backupType),
	)
}

func (b *FileOpener) createBucket(
	ctx context.Context,
	date libtime.Date,
	topic libkafka.Topic,
) error {
	dir := b.toDir(date, topic)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		glog.V(4).Infof("dir '%s' does not exist => create", dir)
		if err := os.MkdirAll(dir, 0750); err != nil {
			return errors.Wrap(ctx, err, "create dir failed")
		}
	}
	return nil
}

func (b *FileOpener) createBucketPartition(
	ctx context.Context,
	topic libkafka.Topic,
	partition libkafka.Partition,
) error {
	dir := b.toDirPartition(topic, partition)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		glog.V(4).Infof("dir '%s' does not exist => create", dir)
		if err := os.MkdirAll(dir, 0750); err != nil {
			return errors.Wrap(ctx, err, "create dir failed")
		}
	}
	return nil
}

// OpenReaderPartition opens a reader for a specific partition backup file.
func (b *FileOpener) OpenReaderPartition(
	ctx context.Context,
	topic libkafka.Topic,
	partition libkafka.Partition,
	backupType BackupType,
) (io.ReadCloser, error) {
	file, err := os.OpenFile(b.toFilePartition(topic, partition, backupType), os.O_RDONLY, 0600)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "open file failed")
	}
	return file, nil
}

// OpenWriterPartition opens a writer for a specific partition backup file.
// If append is true, data is appended; otherwise file is truncated.
func (b *FileOpener) OpenWriterPartition(
	ctx context.Context,
	topic libkafka.Topic,
	partition libkafka.Partition,
	backupType BackupType,
	append bool,
) (io.WriteCloser, error) {
	if err := b.createBucketPartition(ctx, topic, partition); err != nil {
		return nil, errors.Wrap(ctx, err, "create backup dir")
	}

	flags := os.O_CREATE | os.O_WRONLY
	if append {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}

	file, err := os.OpenFile(
		b.toFilePartition(topic, partition, backupType),
		flags,
		0600,
	)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "open file failed")
	}
	return file, nil
}
