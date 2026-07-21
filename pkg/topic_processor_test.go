// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"os"
	"time"

	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/getsentry/sentry-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka-topic-backup/pkg"
)

// mockSentryClient implements libsentry.Client for testing
type mockSentryClient struct{}

func (m *mockSentryClient) CaptureMessage(
	message string,
	hint *sentry.EventHint,
	scope sentry.EventModifier,
) *sentry.EventID {
	return nil
}

func (m *mockSentryClient) CaptureException(
	exception error,
	hint *sentry.EventHint,
	scope sentry.EventModifier,
) *sentry.EventID {
	return nil
}

func (m *mockSentryClient) Close() error { return nil }

func (m *mockSentryClient) Flush(timeout time.Duration) bool { return true }

// mockCurrentTimeGetter implements libtime.CurrentTimeGetter
type mockCurrentTimeGetter struct {
	now time.Time
}

func (m *mockCurrentTimeGetter) Now() time.Time {
	return m.now
}

var _ = Describe("TopicProcessor", func() {
	var (
		ctx           context.Context
		tempDir       string
		backupDate    libtime.Date
		opener        *pkg.FileOpener
		summary       pkg.BackupSummary
		timeGetter    *mockCurrentTimeGetter
		sentryClient  *mockSentryClient
		backupManager pkg.BackupPerformerFunc
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		tempDir, err = os.MkdirTemp("", "topic-processor-test-*")
		Expect(err).NotTo(HaveOccurred())

		backupDate = libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
		opener = pkg.NewFileOpener(tempDir)
		timeGetter = &mockCurrentTimeGetter{
			now: time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
		}
		sentryClient = &mockSentryClient{}
		summary = pkg.NewBackupSummary(1)
	})

	AfterEach(func() {
		if tempDir != "" {
			_ = os.RemoveAll(tempDir)
		}
	})

	Describe("NewTopicProcessor", func() {
		It("creates a TopicProcessor", func() {
			processor := pkg.NewTopicProcessor(
				0,
				backupDate,
				false,
				opener,
				nil,
				nil,
				timeGetter,
				summary,
			)
			Expect(processor).NotTo(BeNil())
		})
	})

	Describe("Process", func() {
		Context("successful backup", func() {
			BeforeEach(func() {
				backupManager = func(ctx context.Context, date libtime.Date, topic libkafka.Topic) (*pkg.BackupResult, error) {
					// Simulate what real backup does - creates directory via OpenWriter
					writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
					if err != nil {
						return nil, err
					}
					writer.Close()
					return &pkg.BackupResult{
						MessageCount: 100,
						OffsetCount:  5,
						Partition:    0,
						FirstOffset:  1000,
						LastOffset:   1099,
					}, nil
				}
			})

			It("completes backup and writes stats", func() {
				processor := pkg.NewTopicProcessor(
					0,
					backupDate,
					false,
					opener,
					backupManager,
					sentryClient,
					timeGetter,
					summary,
				)

				err := processor.Process(ctx, libkafka.Topic("test-topic"))
				Expect(err).NotTo(HaveOccurred())

				Expect(summary.Completed()).To(Equal(int32(1)))
				Expect(summary.Failed()).To(Equal(int32(0)))
				Expect(summary.Skipped()).To(Equal(int32(0)))

				// Verify stats were written
				stats, err := opener.ReadStats(ctx, backupDate, libkafka.Topic("test-topic"))
				Expect(err).NotTo(HaveOccurred())
				Expect(stats.Success).To(BeTrue())
				Expect(stats.MessageCount).To(Equal(int64(100)))
				Expect(stats.OffsetCount).To(Equal(int64(5)))
				Expect(stats.Topic).To(Equal("test-topic"))
				Expect(stats.Partition).To(Equal(libkafka.Partition(0)))
				Expect(stats.FirstOffset).To(Equal(libkafka.Offset(1000)))
				Expect(stats.LastOffset).To(Equal(libkafka.Offset(1099)))
			})
		})

		Context("backup already exists", func() {
			BeforeEach(func() {
				backupManager = func(ctx context.Context, date libtime.Date, topic libkafka.Topic) (*pkg.BackupResult, error) {
					// Simulate what real backup does - creates directory via OpenWriter
					writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
					if err != nil {
						return nil, err
					}
					writer.Close()
					return &pkg.BackupResult{
						MessageCount: 100,
						OffsetCount:  5,
						Partition:    0,
						FirstOffset:  1000,
						LastOffset:   1099,
					}, nil
				}
			})

			It("skips backup when force is false and stats exist with success", func() {
				// First backup
				summary2 := pkg.NewBackupSummary(2)
				processor := pkg.NewTopicProcessor(
					0,
					backupDate,
					false,
					opener,
					backupManager,
					sentryClient,
					timeGetter,
					summary2,
				)

				err := processor.Process(ctx, libkafka.Topic("test-topic"))
				Expect(err).NotTo(HaveOccurred())
				Expect(summary2.Completed()).To(Equal(int32(1)))
				Expect(summary2.Skipped()).To(Equal(int32(0)))

				// Second backup should skip
				err = processor.Process(ctx, libkafka.Topic("test-topic"))
				Expect(err).NotTo(HaveOccurred())
				Expect(summary2.Completed()).To(Equal(int32(2)))
				Expect(summary2.Skipped()).To(Equal(int32(1)))
			})

			It("does not skip when force is true", func() {
				// First backup
				summary2 := pkg.NewBackupSummary(2)
				processor := pkg.NewTopicProcessor(
					0,
					backupDate,
					false,
					opener,
					backupManager,
					sentryClient,
					timeGetter,
					summary2,
				)

				err := processor.Process(ctx, libkafka.Topic("test-topic"))
				Expect(err).NotTo(HaveOccurred())

				// Second backup with force=true should not skip
				forceSummary := pkg.NewBackupSummary(1)
				forceProcessor := pkg.NewTopicProcessor(
					0,
					backupDate,
					true, // force=true
					opener,
					backupManager,
					sentryClient,
					timeGetter,
					forceSummary,
				)

				err = forceProcessor.Process(ctx, libkafka.Topic("test-topic"))
				Expect(err).NotTo(HaveOccurred())
				Expect(forceSummary.Completed()).To(Equal(int32(1)))
				Expect(forceSummary.Skipped()).To(Equal(int32(0)))
			})
		})

		Context("backup failure", func() {
			BeforeEach(func() {
				backupManager = func(ctx context.Context, date libtime.Date, topic libkafka.Topic) (*pkg.BackupResult, error) {
					// Simulate what real backup does - creates directory via OpenWriter, then fails
					writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
					if err != nil {
						return nil, err
					}
					writer.Close()
					return nil, context.DeadlineExceeded
				}
			})

			It("records failure and continues", func() {
				processor := pkg.NewTopicProcessor(
					0,
					backupDate,
					false,
					opener,
					backupManager,
					sentryClient,
					timeGetter,
					summary,
				)

				err := processor.Process(ctx, libkafka.Topic("test-topic"))
				Expect(err).NotTo(HaveOccurred()) // Should not return error for non-fatal failures

				Expect(summary.Completed()).To(Equal(int32(1)))
				Expect(summary.Failed()).To(Equal(int32(1)))
				Expect(summary.FailedTopics()).To(Equal([]libkafka.Topic{"test-topic"}))

				// Verify stats were written with error
				stats, err := opener.ReadStats(ctx, backupDate, libkafka.Topic("test-topic"))
				Expect(err).NotTo(HaveOccurred())
				Expect(stats.Success).To(BeFalse())
				Expect(stats.Error).To(ContainSubstring("deadline exceeded"))
			})
		})

		Context("uses injected time getter", func() {
			BeforeEach(func() {
				backupManager = func(ctx context.Context, date libtime.Date, topic libkafka.Topic) (*pkg.BackupResult, error) {
					// Simulate what real backup does - creates directory via OpenWriter
					writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
					if err != nil {
						return nil, err
					}
					writer.Close()
					return &pkg.BackupResult{}, nil
				}
			})

			It("records correct timestamps from injected time", func() {
				startTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
				timeGetter.now = startTime

				processor := pkg.NewTopicProcessor(
					0,
					backupDate,
					false,
					opener,
					backupManager,
					sentryClient,
					timeGetter,
					summary,
				)

				err := processor.Process(ctx, libkafka.Topic("test-topic"))
				Expect(err).NotTo(HaveOccurred())

				stats, err := opener.ReadStats(ctx, backupDate, libkafka.Topic("test-topic"))
				Expect(err).NotTo(HaveOccurred())
				Expect(stats.StartedAt).To(Equal(startTime))
				Expect(stats.CompletedAt).To(Equal(startTime))
			})
		})
	})
})
