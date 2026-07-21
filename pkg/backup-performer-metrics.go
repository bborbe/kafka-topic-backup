// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"sync"

	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	namespace = "strimzi"
	subsystem = "backup_topic"

	registerOnce sync.Once

	started = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "started",
		Help:      "started",
	}, []string{"topic"})
	completed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "completed",
		Help:      "completed",
	}, []string{"topic"})
	failed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "failed",
		Help:      "failed",
	}, []string{"topic"})
	lastSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "last_success",
		Help:      "Timestamp of last successful run",
	}, []string{"topic"})
	messageCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "message_count",
		Help:      "Number of messages backed up",
	}, []string{"topic"})
	durationSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "duration_seconds",
		Help:      "Duration of backup in seconds",
	}, []string{"topic"})
)

func NewBackupPerformerMetrics(
	prometheusRegister prometheus.Registerer,
	backupPerformer BackupPerformer,
) BackupPerformer {
	registerOnce.Do(func() {
		prometheusRegister.MustRegister(
			started,
			completed,
			failed,
			lastSuccess,
			messageCount,
			durationSeconds,
		)
	})
	return BackupPerformerFunc(
		func(ctx context.Context, backupDate libtime.Date, topic libkafka.Topic) (*BackupResult, error) {
			started.With(prometheus.Labels{"topic": topic.String()}).Inc()
			result, err := backupPerformer.Backup(ctx, backupDate, topic)
			if err != nil {
				failed.With(prometheus.Labels{"topic": topic.String()}).Inc()
				return nil, err
			}
			completed.With(prometheus.Labels{"topic": topic.String()}).Inc()
			lastSuccess.With(prometheus.Labels{"topic": topic.String()}).SetToCurrentTime()
			if result != nil {
				messageCount.With(prometheus.Labels{"topic": topic.String()}).
					Set(float64(result.MessageCount))
			}
			return result, nil
		},
	)
}
