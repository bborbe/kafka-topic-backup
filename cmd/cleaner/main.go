// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/metrics"
	libsentry "github.com/bborbe/sentry"
	"github.com/bborbe/service"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/kafka-topic-backup/pkg"
)

func main() {
	app := &application{}
	os.Exit(service.Main(context.Background(), app, &app.SentryDSN, &app.SentryProxy))
}

type application struct {
	SentryDSN       string            `required:"false" arg:"sentry-dsn"        env:"SENTRY_DSN"        usage:"SentryDSN"                 display:"length"`
	SentryProxy     string            `required:"false" arg:"sentry-proxy"      env:"SENTRY_PROXY"      usage:"Sentry Proxy"`
	DataDir         string            `required:"true"  arg:"datadir"           env:"DATADIR"           usage:"data directory"                             default:"./data"`
	DryRun          bool              `required:"false" arg:"dry-run"           env:"DRY_RUN"           usage:"dry run mode"                               default:"false"`
	BuildGitVersion string            `required:"false" arg:"build-git-version" env:"BUILD_GIT_VERSION" usage:"Build Git version"                          default:"dev"`
	BuildGitCommit  string            `required:"false" arg:"build-git-commit"  env:"BUILD_GIT_COMMIT"  usage:"Build Git commit hash"                      default:"none"`
	BuildDate       *libtime.DateTime `required:"false" arg:"build-date"        env:"BUILD_DATE"        usage:"Build timestamp (RFC3339)"`
}

func (a *application) Run(ctx context.Context, sentryClient libsentry.Client) error {
	metrics.NewBuildInfoMetrics().SetBuildInfo(a.BuildGitVersion, a.BuildGitCommit, a.BuildDate)

	glog.V(1).Infof("checking backups in %s (dry-run: %v)", a.DataDir, a.DryRun)

	opener := pkg.NewFileOpener(a.DataDir)

	// Discover topics
	topics, err := a.discoverTopics(ctx)
	if err != nil {
		return errors.Wrap(ctx, err, "discover topics failed")
	}

	var failedCount, successCount int

	for _, topicName := range topics {
		topic := libkafka.Topic(topicName)
		partitions, err := a.discoverPartitions(ctx, topicName)
		if err != nil {
			glog.Warningf("failed to discover partitions for %s: %v", topic, err)
			continue
		}

		for _, p := range partitions {
			partition := libkafka.Partition(
				p,
			) //#nosec G115 -- partition numbers from Kafka are always valid int32
			stats, err := opener.ReadStatsPartition(ctx, topic, partition)
			if err != nil {
				glog.V(1).Infof("%s:%d - no stats.json", topic, partition)
				failedCount++
				continue
			}

			if !stats.Success {
				glog.V(1).Infof("%s:%d - failed backup (error: %s)", topic, partition, stats.Error)
				failedCount++

				if !a.DryRun {
					// Clean up failed backup
					partitionDir := filepath.Join(a.DataDir, topicName, strconv.Itoa(p))
					if err := os.RemoveAll(partitionDir); err != nil {
						glog.Warningf("failed to remove %s: %v", partitionDir, err)
					} else {
						glog.V(1).Infof("removed failed backup: %s", partitionDir)
					}
				}
				continue
			}

			successCount++
			glog.V(2).Infof(
				"%s:%d - OK (messages: %d, offsets: %d-%d)",
				topic, partition, stats.MessageCount, stats.FirstOffset, stats.LastOffset,
			)
		}
	}

	glog.Infof("Backup status: %d successful, %d failed/incomplete", successCount, failedCount)
	return nil
}

// discoverTopics finds all topic directories in the backup.
func (a *application) discoverTopics(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(a.DataDir)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "read data dir %s failed", a.DataDir)
	}

	var topics []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		topics = append(topics, entry.Name())
	}
	return topics, nil
}

// discoverPartitions finds all partition directories for a topic.
func (a *application) discoverPartitions(ctx context.Context, topic string) ([]int, error) {
	topicDir := filepath.Join(a.DataDir, topic)
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, errors.Wrapf(ctx, err, "read topic dir %s failed", topicDir)
	}

	var partitions []int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		p, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		partitions = append(partitions, p)
	}
	return partitions, nil
}
