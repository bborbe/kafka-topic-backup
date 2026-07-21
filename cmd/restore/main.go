// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/metrics"
	libsentry "github.com/bborbe/sentry"
	"github.com/bborbe/service"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/kafka-topic-backup/avro"
	"github.com/bborbe/kafka-topic-backup/pkg"
)

func main() {
	app := &application{}
	os.Exit(service.Main(context.Background(), app, &app.SentryDSN, &app.SentryProxy))
}

type application struct {
	SentryDSN       string            `required:"false" arg:"sentry-dsn"        env:"SENTRY_DSN"        usage:"SentryDSN"                             display:"length"`
	SentryProxy     string            `required:"false" arg:"sentry-proxy"      env:"SENTRY_PROXY"      usage:"Sentry Proxy"`
	KafkaBrokers    libkafka.Brokers  `required:"true"  arg:"kafka-brokers"     env:"KAFKA_BROKERS"     usage:"Comma separated list of Kafka brokers"`
	DataDir         string            `required:"true"  arg:"datadir"           env:"DATADIR"           usage:"data directory"                                         default:"./data"`
	SourceTopic     libkafka.Topic    `required:"true"  arg:"source-topic"      env:"SOURCE_TOPIC"      usage:"source topic name from backup"`
	TargetTopic     libkafka.Topic    `required:"true"  arg:"target-topic"      env:"TARGET_TOPIC"      usage:"target topic to restore to"`
	BuildGitVersion string            `required:"false" arg:"build-git-version" env:"BUILD_GIT_VERSION" usage:"Build Git version"                                      default:"dev"`
	BuildGitCommit  string            `required:"false" arg:"build-git-commit"  env:"BUILD_GIT_COMMIT"  usage:"Build Git commit hash"                                  default:"none"`
	BuildDate       *libtime.DateTime `required:"false" arg:"build-date"        env:"BUILD_DATE"        usage:"Build timestamp (RFC3339)"`
}

func (a *application) Run(ctx context.Context, sentryClient libsentry.Client) error {
	metrics.NewBuildInfoMetrics().SetBuildInfo(a.BuildGitVersion, a.BuildGitCommit, a.BuildDate)

	// Create Kafka client provider
	saramaClientProvider, err := libkafka.NewSaramaClientProviderByType(
		ctx,
		libkafka.SaramaClientProviderTypeNew,
		a.KafkaBrokers,
	)
	if err != nil {
		return errors.Wrap(ctx, err, "create sarama client provider failed")
	}
	defer saramaClientProvider.Close()

	saramaClient, err := saramaClientProvider.Client(ctx)
	if err != nil {
		return errors.Wrap(ctx, err, "create sarama client failed")
	}

	// Create file opener
	opener := pkg.NewFileOpener(a.DataDir)

	// Discover partitions from backup directory
	partitions, err := a.discoverPartitions(ctx, a.SourceTopic)
	if err != nil {
		return errors.Wrap(ctx, err, "discover partitions failed")
	}

	glog.V(1).Infof(
		"restoring topic %s (%d partitions) from %s to %s",
		a.SourceTopic,
		len(partitions),
		a.DataDir,
		a.TargetTopic,
	)

	// Create Kafka producer
	producer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		return errors.Wrap(ctx, err, "create producer failed")
	}
	defer producer.Close()

	// Restore each partition
	totalMessages := 0
	for _, partition := range partitions {
		count, err := a.restorePartition(
			ctx,
			opener,
			producer,
			a.SourceTopic,
			partition,
			a.TargetTopic,
		)
		if err != nil {
			return errors.Wrapf(ctx, err, "restore partition %d failed", partition)
		}
		totalMessages += count
		glog.V(1).Infof("restored partition %d: %d messages", partition, count)
	}

	glog.V(1).
		Infof("restore completed: %d total messages across %d partitions", totalMessages, len(partitions))
	return nil
}

// discoverPartitions finds all partition directories in the backup.
func (a *application) discoverPartitions(
	ctx context.Context,
	topic libkafka.Topic,
) ([]libkafka.Partition, error) {
	topicDir := filepath.Join(a.DataDir, topic.String())
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "read topic dir %s failed", topicDir)
	}

	var partitions []libkafka.Partition
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		p, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue // skip non-numeric directories
		}
		partitions = append(
			partitions,
			libkafka.Partition(p),
		) //#nosec G115 -- partition numbers from Kafka are always valid int32
	}
	return partitions, nil
}

// restorePartition restores a single partition's messages.
func (a *application) restorePartition(
	ctx context.Context,
	opener *pkg.FileOpener,
	producer sarama.SyncProducer,
	sourceTopic libkafka.Topic,
	partition libkafka.Partition,
	targetTopic libkafka.Topic,
) (int, error) {
	reader, err := opener.OpenReaderPartition(ctx, sourceTopic, partition, pkg.BackupTypeTopic)
	if err != nil {
		return 0, errors.Wrap(ctx, err, "open partition backup failed")
	}
	defer reader.Close()

	messageCount := 0
	for {
		select {
		case <-ctx.Done():
			return messageCount, ctx.Err()
		default:
		}

		record, err := avro.DeserializeBackupTopicRecord(reader)
		if err != nil {
			if err == io.EOF {
				return messageCount, nil
			}
			return messageCount, errors.Wrap(ctx, err, "deserialize record failed")
		}

		msg := &sarama.ProducerMessage{
			Topic:     string(targetTopic),
			Key:       sarama.ByteEncoder(record.Key),
			Value:     sarama.ByteEncoder(record.Value),
			Timestamp: time.UnixMilli(record.Timestamp),
		}

		for _, header := range record.Headers {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   header.Key,
				Value: header.Value,
			})
		}

		if _, _, err := producer.SendMessage(msg); err != nil {
			return messageCount, errors.Wrapf(ctx, err, "send message failed")
		}

		messageCount++
		if messageCount%10000 == 0 {
			glog.V(2).Infof("restored %d messages for partition %d", messageCount, partition)
		}
	}
}
