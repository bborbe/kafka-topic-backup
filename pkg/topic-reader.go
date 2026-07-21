// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/log"
	"github.com/bborbe/run"
	libsentry "github.com/bborbe/sentry"
	"github.com/golang/glog"

	"github.com/bborbe/kafka-topic-backup/avro"
)

// TopicReader reads messages from Kafka topics for backup.
//
//counterfeiter:generate -o ../../mocks/topic-reader.go --fake-name TopicReader . TopicReader
type TopicReader interface {
	Read(ctx context.Context, topic libkafka.Topic, ch chan<- avro.BackupTopicRecord) error
}

func NewTopicReader(
	sentryClient libsentry.Client,
	saramaClientProvider libkafka.SaramaClientProvider,
	kafkaBrokers libkafka.Brokers,
	batchSize libkafka.BatchSize,
	converter ConsumerMessageConverter,
	logSamplerFactory log.SamplerFactory,
) TopicReader {
	return &topicReader{
		logSamplerFactory:    logSamplerFactory,
		sentryClient:         sentryClient,
		saramaClientProvider: saramaClientProvider,
		converter:            converter,
		kafkaBrokers:         kafkaBrokers,
		batchSize:            batchSize,
	}
}

type topicReader struct {
	logSamplerFactory    log.SamplerFactory
	converter            ConsumerMessageConverter
	kafkaBrokers         libkafka.Brokers
	sentryClient         libsentry.Client
	batchSize            libkafka.BatchSize
	saramaClientProvider libkafka.SaramaClientProvider
}

func (t *topicReader) Read(
	ctx context.Context,
	topic libkafka.Topic,
	ch chan<- avro.BackupTopicRecord,
) error {
	glog.V(2).Infof("read topic %v started", topic)
	if err := t.read(ctx, topic, ch); err != nil {
		glog.V(1).Infof("read topic %v failed: %v", topic, err)
		return errors.Wrapf(ctx, err, "read topic %v failed", topic)
	}
	glog.V(2).Infof("read topic %v completed", topic)
	return nil
}

func (t *topicReader) read(
	ctx context.Context,
	topic libkafka.Topic,
	ch chan<- avro.BackupTopicRecord,
) error {
	saramaClient, err := t.saramaClientProvider.Client(ctx)
	if err != nil {
		return errors.Wrap(ctx, err, "get sarama client failed")
	}
	defer saramaClient.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	trigger := run.NewTrigger()
	go func() {
		select {
		case <-ctx.Done():
		case <-trigger.Done():
			glog.V(2).Infof("reached highwater mark offsets")
			cancel()
		}
	}()

	err = libkafka.NewOffsetConsumerHighwaterMarksBatch(
		saramaClient,
		topic,
		libkafka.NewSaramaOffsetManager(
			saramaClient,
			"strimzi-topic-backuper",
			libkafka.OffsetOldest,
			libkafka.OffsetNewest,
		),
		libkafka.NewMessageHandlerBatch(
			libkafka.MessageHandlerFunc(
				func(ctx context.Context, msg *sarama.ConsumerMessage) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case ch <- t.converter.Convert(msg):
						return nil
					}
				},
			),
		),
		t.batchSize,
		trigger,
		t.logSamplerFactory,
	).Consume(ctx)

	if err != nil && !errors.Is(err, context.Canceled) {
		return errors.Wrap(ctx, err, "consume failed")
	}
	glog.V(2).Infof("consumed all messages up to highwater mark")
	return nil
}
