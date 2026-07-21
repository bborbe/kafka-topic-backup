// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	"github.com/bborbe/metrics"
	"github.com/bborbe/run"
	libsentry "github.com/bborbe/sentry"
	"github.com/bborbe/service"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/kafka-topic-backup/pkg"
	"github.com/bborbe/kafka-topic-backup/pkg/factory"
)

func main() {
	app := &application{}
	os.Exit(service.Main(context.Background(), app, &app.SentryDSN, &app.SentryProxy))
}

type application struct {
	SentryDSN       string             `required:"false" arg:"sentry-dsn"        env:"SENTRY_DSN"        usage:"SentryDSN"                                             display:"length"`
	SentryProxy     string             `required:"false" arg:"sentry-proxy"      env:"SENTRY_PROXY"      usage:"Sentry Proxy"`
	KafkaBrokers    libkafka.Brokers   `required:"true"  arg:"kafka-brokers"     env:"KAFKA_BROKERS"     usage:"Comma separated list of Kafka brokers"`
	BatchSize       libkafka.BatchSize `required:"true"  arg:"batch-size"        env:"BATCH_SIZE"        usage:"batch consume size"                                                     default:"250"`
	ScanTopics      []libkafka.Topic   `required:"true"  arg:"scan-topics"       env:"SCAN_TOPICS"       usage:"comma-separated list of topics to scan for corruption"`
	Partition       int                `required:"false" arg:"partition"         env:"PARTITION"         usage:"specific partition to scan (default: all partitions)"                   default:"-1"`
	StartOffset     int64              `required:"false" arg:"start-offset"      env:"START_OFFSET"      usage:"start scanning from this offset (default: oldest)"                      default:"-1"`
	Workers         int                `required:"false" arg:"workers"           env:"WORKERS"           usage:"number of parallel scan workers"                                        default:"5"`
	BuildGitVersion string             `required:"false" arg:"build-git-version" env:"BUILD_GIT_VERSION" usage:"Build Git version"                                                      default:"dev"`
	BuildGitCommit  string             `required:"false" arg:"build-git-commit"  env:"BUILD_GIT_COMMIT"  usage:"Build Git commit hash"                                                  default:"none"`
	BuildDate       *libtime.DateTime  `required:"false" arg:"build-date"        env:"BUILD_DATE"        usage:"Build timestamp (RFC3339)"`
}

func (a *application) Run(ctx context.Context, sentryClient libsentry.Client) error {
	metrics.NewBuildInfoMetrics().SetBuildInfo(a.BuildGitVersion, a.BuildGitCommit, a.BuildDate)

	currentTimeGetter := libtime.NewCurrentTime()

	saramaConfigOptions := factory.CreateSaramaConfigOptions(a.BatchSize)

	// Create Kafka client provider with pool (health-checked connections)
	saramaClientProvider, err := libkafka.NewSaramaClientProviderByType(
		ctx,
		libkafka.SaramaClientProviderTypePool,
		a.KafkaBrokers,
		saramaConfigOptions,
	)
	if err != nil {
		return errors.Wrap(ctx, err, "create sarama client provider failed")
	}
	defer saramaClientProvider.Close()

	glog.V(1).Infof(
		"Scanning %d topics with %d workers",
		len(a.ScanTopics), a.Workers,
	)

	summary := pkg.NewScanSummary(len(a.ScanTopics))

	// Create topic queue
	topicChan := make(chan libkafka.Topic, len(a.ScanTopics))

	// Create runs: producer + workers
	runs := []run.Func{
		// Producer goroutine
		func(ctx context.Context) error {
			for _, topic := range a.ScanTopics {
				topicChan <- topic
			}
			close(topicChan)
			return nil
		},
	}

	// Worker goroutines
	for i := 0; i < a.Workers; i++ {
		worker := createWorker(
			i,
			saramaClientProvider,
			currentTimeGetter,
			a.Partition,
			a.StartOffset,
			topicChan,
			summary,
		)
		runs = append(runs, worker)
	}

	// Run all runs (cancels on first error)
	if err := run.CancelOnFirstError(ctx, runs...); err != nil {
		return errors.Wrap(ctx, err, "scan failed")
	}

	// Print summary
	glog.Infof("")
	glog.Infof("=" + string(make([]byte, 60)))
	glog.Infof("Scan Summary")
	glog.Infof("=" + string(make([]byte, 60)))
	glog.Infof("Topics scanned: %d", summary.Total())
	glog.Infof("Corrupt ranges found: %d", summary.TotalCorruptRanges())
	glog.Infof("")

	// Print details for each topic
	for _, result := range summary.Results() {
		glog.Infof("%s", pkg.FormatScanResult(-1, result))
	}

	return nil
}

func createWorker(
	id int,
	saramaClientProvider libkafka.SaramaClientProvider,
	currentTimeGetter libtime.CurrentTimeGetter,
	partition int,
	startOffset int64,
	topics <-chan libkafka.Topic,
	summary pkg.ScanSummary,
) run.Func {
	return func(ctx context.Context) error {
		corruptionSkipper := pkg.NewCorruptionSkipper(log.DefaultSamplerFactory)
		scanner := pkg.NewCorruptionScanner(
			saramaClientProvider,
			currentTimeGetter,
			partition,
			startOffset,
			corruptionSkipper,
		)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case topic, ok := <-topics:
				if !ok {
					glog.V(2).Infof("Worker %d finished", id)
					return nil
				}

				glog.V(1).Infof("Worker %d: Scanning %s", id, topic)

				result, err := scanner.Scan(ctx, topic)
				if err != nil {
					glog.Warningf("Worker %d: Failed to scan %s: %v", id, topic, err)
					return errors.Wrapf(ctx, err, "scan %s failed", topic)
				}

				summary.AddResult(result)
				current := summary.AddCompleted()

				glog.Infof("Worker %d [%d/%d]: Completed %s", id, current, summary.Total(), topic)
				glog.V(1).Infof("%s", pkg.FormatScanResult(id, result))
			}
		}
	}
}
