// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka-topic-backup/pkg"
)

// mockTopicProcessor implements TopicProcessor for testing
type mockTopicProcessor struct {
	processedTopics []libkafka.Topic
	returnError     error
}

func (m *mockTopicProcessor) Process(ctx context.Context, topic libkafka.Topic) error {
	m.processedTopics = append(m.processedTopics, topic)
	return m.returnError
}

var _ = Describe("Worker", func() {
	var (
		ctx       context.Context
		processor *mockTopicProcessor
	)

	BeforeEach(func() {
		ctx = context.Background()
		processor = &mockTopicProcessor{}
	})

	Describe("NewWorker", func() {
		It("creates a worker", func() {
			topics := make(chan libkafka.Topic)
			worker := pkg.NewWorker(processor, topics)
			Expect(worker).NotTo(BeNil())
		})
	})

	Context("processing topics", func() {
		It("processes all topics from channel", func() {
			topics := make(chan libkafka.Topic, 3)
			topics <- libkafka.Topic("topic-1")
			topics <- libkafka.Topic("topic-2")
			topics <- libkafka.Topic("topic-3")
			close(topics)

			worker := pkg.NewWorker(processor, topics)
			err := worker(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(processor.processedTopics).To(HaveLen(3))
			Expect(processor.processedTopics[0]).To(Equal(libkafka.Topic("topic-1")))
			Expect(processor.processedTopics[1]).To(Equal(libkafka.Topic("topic-2")))
			Expect(processor.processedTopics[2]).To(Equal(libkafka.Topic("topic-3")))
		})

		It("handles empty channel", func() {
			topics := make(chan libkafka.Topic)
			close(topics)

			worker := pkg.NewWorker(processor, topics)
			err := worker(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(processor.processedTopics).To(BeEmpty())
		})

		It("returns error when processor fails", func() {
			processor.returnError = errors.New(ctx, "process failed")

			topics := make(chan libkafka.Topic, 1)
			topics <- libkafka.Topic("failing-topic")
			close(topics)

			worker := pkg.NewWorker(processor, topics)
			err := worker(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("process topic failing-topic failed"))
		})

		It("stops on context cancellation", func() {
			cancelCtx, cancel := context.WithCancel(ctx)

			topics := make(chan libkafka.Topic, 2)
			topics <- libkafka.Topic("topic-1")
			topics <- libkafka.Topic("topic-2")
			// Don't close - worker should exit on context cancel

			// Cancel after first topic is sent
			processor = &mockTopicProcessor{}

			worker := pkg.NewWorker(processor, topics)

			// Start worker in goroutine
			errChan := make(chan error, 1)
			go func() {
				errChan <- worker(cancelCtx)
			}()

			// Cancel context immediately
			cancel()

			// Worker should return with context error
			Eventually(errChan).Should(Receive(Equal(context.Canceled)))
		})
	})
})
