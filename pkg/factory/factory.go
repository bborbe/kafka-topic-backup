// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package factory

import (
	"time"

	"github.com/IBM/sarama"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	"github.com/bborbe/run"
	libsentry "github.com/bborbe/sentry"
	libtime "github.com/bborbe/time"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/bborbe/kafka-topic-backup/pkg"
)

func CreateSaramaConfigOptions(batchSize libkafka.BatchSize) libkafka.SaramaConfigOptions {
	return func(config *sarama.Config) {
		config.Consumer.MaxWaitTime = 1000 * time.Millisecond
		config.Consumer.Fetch.Default = 10 * 1024 * 1024
		config.Consumer.Fetch.Max = 50 * 1024 * 1024
		config.ChannelBufferSize = batchSize.Int() * 5
	}
}

func CreateWorker(
	id int,
	backupDate libtime.Date,
	kafkaBrokers libkafka.Brokers,
	batchSize libkafka.BatchSize,
	dataDir string,
	force bool,
	registerer prometheus.Registerer,
	sentryClient libsentry.Client,
	currentTimeGetter libtime.CurrentTimeGetter,
	saramaClientProvider libkafka.SaramaClientProvider,
	topics <-chan libkafka.Topic,
	summary pkg.BackupSummary,
) run.Func {
	return pkg.NewWorker(
		pkg.NewTopicProcessor(
			id,
			backupDate,
			force,
			pkg.NewFileOpener(dataDir),
			pkg.NewBackupPerformerMetrics(
				registerer,
				pkg.BackupPerformerList{
					CreateBackupPerformerTopic(
						sentryClient,
						saramaClientProvider,
						kafkaBrokers,
						batchSize,
						dataDir,
					),
					CreateBackupPerformerOffset(saramaClientProvider, dataDir),
				},
			),
			sentryClient,
			currentTimeGetter,
			summary,
		),
		topics,
	)
}

func CreateBackupPerformerOffset(
	saramaClientProvider libkafka.SaramaClientProvider,
	dataDir string,
) pkg.BackupPerformer {
	return pkg.NewBackupPerformerOffset(
		pkg.NewConsumerGroupOffsetReader(
			saramaClientProvider,
			log.DefaultSamplerFactory,
		),
		pkg.NewFileOpener(dataDir),
		log.DefaultSamplerFactory,
	)
}

func CreateBackupPerformerTopic(
	sentryClient libsentry.Client,
	saramaClientProvider libkafka.SaramaClientProvider,
	kafkaBrokers libkafka.Brokers,
	batchSize libkafka.BatchSize,
	dataDir string,
) pkg.BackupPerformer {
	return pkg.NewBackupPerformerTopic(
		pkg.NewTopicReader(
			sentryClient,
			saramaClientProvider,
			kafkaBrokers,
			batchSize,
			pkg.NewConsumerMessageConverter(),
			log.DefaultSamplerFactory,
		),
		pkg.NewFileOpener(dataDir),
		log.DefaultSamplerFactory,
	)
}

// CreatePartitionWorker creates a worker that processes partition backup tasks.
func CreatePartitionWorker(
	id int,
	dataDir string,
	skipCorrupt bool,
	sentryClient libsentry.Client,
	currentTimeGetter libtime.CurrentTimeGetter,
	saramaClientProvider libkafka.SaramaClientProvider,
	tasks <-chan pkg.PartitionTask,
	summary pkg.BackupSummary,
) run.Func {
	opener := pkg.NewFileOpener(dataDir)
	corruptionSkipper := pkg.NewCorruptionSkipper(log.DefaultSamplerFactory)
	return pkg.NewPartitionWorker(
		pkg.NewPartitionProcessor(
			id,
			opener,
			pkg.NewBackupPerformerPartition(
				pkg.NewPartitionReader(
					saramaClientProvider,
					pkg.NewConsumerMessageConverter(),
					log.DefaultSamplerFactory,
					corruptionSkipper,
				),
				opener,
				log.DefaultSamplerFactory,
				skipCorrupt,
			),
			sentryClient,
			currentTimeGetter,
			summary,
		),
		tasks,
	)
}
