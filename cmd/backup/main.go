// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"syscall"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/metrics"
	"github.com/bborbe/run"
	libsentry "github.com/bborbe/sentry"
	"github.com/bborbe/service"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/kafka-topic-backup/pkg"
	"github.com/bborbe/kafka-topic-backup/pkg/factory"
)

const lockFile = "/tmp/kafka-backup.lock"

func main() {
	app := &application{}
	os.Exit(service.Main(context.Background(), app, &app.SentryDSN, &app.SentryProxy))
}

type application struct {
	SentryDSN       string            `required:"false" arg:"sentry-dsn"        env:"SENTRY_DSN"        usage:"SentryDSN"                                display:"length"`
	SentryProxy     string            `required:"false" arg:"sentry-proxy"      env:"SENTRY_PROXY"      usage:"Sentry Proxy"`
	KafkaBrokers    libkafka.Brokers  `required:"true"  arg:"kafka-brokers"     env:"KAFKA_BROKERS"     usage:"Comma separated list of Kafka brokers"`
	DataDir         string            `required:"true"  arg:"datadir"           env:"DATADIR"           usage:"data directory"                                            default:"./data"`
	BackupTopics    []libkafka.Topic  `required:"true"  arg:"backup-topics"     env:"BACKUP_TOPICS"     usage:"comma-separated list of topics to backup"`
	Workers         int               `required:"false" arg:"workers"           env:"WORKERS"           usage:"number of parallel backup workers"                         default:"10"`
	SkipCorrupt     bool              `required:"false" arg:"skip-corrupt"      env:"SKIP_CORRUPT"      usage:"skip corrupt records instead of failing"                   default:"false"`
	BuildGitVersion string            `required:"false" arg:"build-git-version" env:"BUILD_GIT_VERSION" usage:"Build Git version"                                         default:"dev"`
	BuildGitCommit  string            `required:"false" arg:"build-git-commit"  env:"BUILD_GIT_COMMIT"  usage:"Build Git commit hash"                                     default:"none"`
	BuildDate       *libtime.DateTime `required:"false" arg:"build-date"        env:"BUILD_DATE"        usage:"Build timestamp (RFC3339)"`
}

func (a *application) Run(ctx context.Context, sentryClient libsentry.Client) error {
	f, err := os.OpenFile(lockFile, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return errors.Wrap(ctx, err, "open lock file failed")
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil { // #nosec G115 -- fd fits in int on all supported platforms
		return errors.Wrapf(ctx, err, "backup already running (lock held by another process)")
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }() // #nosec G115 -- fd fits in int on all supported platforms
	glog.V(1).Infof("acquired lock %s", lockFile)

	metrics.NewBuildInfoMetrics().SetBuildInfo(a.BuildGitVersion, a.BuildGitCommit, a.BuildDate)

	currentTimeGetter := libtime.NewCurrentTime()

	// Create Kafka client provider
	saramaClientProvider, err := libkafka.NewSaramaClientProviderByType(
		ctx,
		libkafka.SaramaClientProviderTypePool,
		a.KafkaBrokers,
	)
	if err != nil {
		return errors.Wrap(ctx, err, "create sarama client provider failed")
	}
	defer saramaClientProvider.Close()

	// Discover partitions for all topics
	tasks, err := a.discoverPartitions(ctx, saramaClientProvider)
	if err != nil {
		return errors.Wrap(ctx, err, "discover partitions failed")
	}

	glog.V(1).Infof(
		"backing up %d partitions from %d topics with %d workers to %s",
		len(tasks), len(a.BackupTopics), a.Workers, a.DataDir,
	)

	summary := pkg.NewBackupSummary(len(tasks))

	// Create task queue
	taskChan := make(chan pkg.PartitionTask, len(tasks))

	// Create runs: producer + workers
	runs := []run.Func{
		func(ctx context.Context) error {
			for _, task := range tasks {
				taskChan <- task
			}
			close(taskChan)
			return nil
		},
	}
	for i := 0; i < a.Workers; i++ {
		worker := factory.CreatePartitionWorker(
			i,
			a.DataDir,
			a.SkipCorrupt,
			sentryClient,
			currentTimeGetter,
			saramaClientProvider,
			taskChan,
			summary,
		)
		runs = append(runs, worker)
	}

	// Run all (cancels on first error)
	if err := run.CancelOnFirstError(ctx, runs...); err != nil {
		return errors.Wrap(ctx, err, "backup failed")
	}

	// Print summary
	glog.Infof(
		"Backup summary: %d total, %d succeeded, %d skipped, %d crc, %d failed",
		summary.Total(),
		summary.Succeeded(),
		summary.Skipped(),
		summary.CRCTopicCount(),
		summary.Failed(),
	)

	// Print failed topics if any
	if summary.Failed() > 0 {
		for _, topic := range summary.FailedTopics() {
			glog.Infof("  Failed: %s", topic)
		}
	}

	// Print CRC errors by topic
	a.printCRCErrorSummary(summary)

	return nil
}

// printCRCErrorSummary displays CRC errors grouped by topic and sorted by total error count.
func (a *application) printCRCErrorSummary(summary pkg.BackupSummary) {
	crcErrors := summary.CRCErrors()
	if len(crcErrors) == 0 {
		return
	}

	// Calculate total errors per topic and sort topics
	type topicTotal struct {
		topic libkafka.Topic
		total int
	}
	var topicTotals []topicTotal
	for topic, partitions := range crcErrors {
		total := 0
		for _, count := range partitions {
			total += count
		}
		topicTotals = append(topicTotals, topicTotal{topic: topic, total: total})
	}

	// Sort by total error count descending
	for i := 0; i < len(topicTotals); i++ {
		for j := i + 1; j < len(topicTotals); j++ {
			if topicTotals[j].total > topicTotals[i].total {
				topicTotals[i], topicTotals[j] = topicTotals[j], topicTotals[i]
			}
		}
	}

	glog.Info("")
	glog.Info("CRC errors by topic:")
	for _, tt := range topicTotals {
		glog.Infof("  %s:", tt.topic)
		// Sort partitions by partition number
		partitions := crcErrors[tt.topic]
		var partitionNums []libkafka.Partition
		for p := range partitions {
			partitionNums = append(partitionNums, p)
		}
		for i := 0; i < len(partitionNums); i++ {
			for j := i + 1; j < len(partitionNums); j++ {
				if partitionNums[j] < partitionNums[i] {
					partitionNums[i], partitionNums[j] = partitionNums[j], partitionNums[i]
				}
			}
		}
		for _, p := range partitionNums {
			glog.Infof("    Partition %d: %d ranges", p, partitions[p])
		}
	}
}

// discoverPartitions queries Kafka for all partitions of each topic.
func (a *application) discoverPartitions(
	ctx context.Context,
	provider libkafka.SaramaClientProvider,
) ([]pkg.PartitionTask, error) {
	client, err := provider.Client(ctx)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "get client failed")
	}
	defer client.Close()

	var tasks []pkg.PartitionTask
	for _, topic := range a.BackupTopics {
		partitions, err := client.Partitions(topic.String())
		if err != nil {
			return nil, errors.Wrapf(ctx, err, "get partitions for %s failed", topic)
		}
		glog.V(2).Infof("topic %s has %d partitions", topic, len(partitions))
		for _, p := range partitions {
			tasks = append(tasks, pkg.PartitionTask{
				Topic:     topic,
				Partition: libkafka.Partition(p),
			})
		}
	}
	return tasks, nil
}
