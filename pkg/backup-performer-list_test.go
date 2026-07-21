// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"time"

	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka-topic-backup/pkg"
)

var _ = Describe("BackupPerformerList", func() {
	var (
		ctx        context.Context
		backupDate libtime.Date
		topic      libkafka.Topic
	)

	BeforeEach(func() {
		ctx = context.Background()
		backupDate = libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
		topic = libkafka.Topic("test-topic")
	})

	Describe("Backup", func() {
		Context("with empty list", func() {
			It("returns empty result", func() {
				list := pkg.BackupPerformerList{}
				result, err := list.Backup(ctx, backupDate, topic)
				Expect(err).NotTo(HaveOccurred())
				Expect(result).NotTo(BeNil())
				Expect(result.MessageCount).To(Equal(int64(0)))
				Expect(result.OffsetCount).To(Equal(int64(0)))
			})
		})

		Context("with single performer", func() {
			It("returns performer result", func() {
				performer := pkg.BackupPerformerFunc(
					func(ctx context.Context, date libtime.Date, t libkafka.Topic) (*pkg.BackupResult, error) {
						return &pkg.BackupResult{
							MessageCount: 100,
							OffsetCount:  5,
						}, nil
					},
				)

				list := pkg.BackupPerformerList{performer}
				result, err := list.Backup(ctx, backupDate, topic)
				Expect(err).NotTo(HaveOccurred())
				Expect(result.MessageCount).To(Equal(int64(100)))
				Expect(result.OffsetCount).To(Equal(int64(5)))
			})
		})

		Context("with multiple performers", func() {
			It("aggregates results", func() {
				performer1 := pkg.BackupPerformerFunc(
					func(ctx context.Context, date libtime.Date, t libkafka.Topic) (*pkg.BackupResult, error) {
						return &pkg.BackupResult{
							MessageCount: 100,
							OffsetCount:  0,
						}, nil
					},
				)
				performer2 := pkg.BackupPerformerFunc(
					func(ctx context.Context, date libtime.Date, t libkafka.Topic) (*pkg.BackupResult, error) {
						return &pkg.BackupResult{
							MessageCount: 0,
							OffsetCount:  10,
						}, nil
					},
				)

				list := pkg.BackupPerformerList{performer1, performer2}
				result, err := list.Backup(ctx, backupDate, topic)
				Expect(err).NotTo(HaveOccurred())
				Expect(result.MessageCount).To(Equal(int64(100)))
				Expect(result.OffsetCount).To(Equal(int64(10)))
			})
		})

		Context("when performer returns error", func() {
			It("returns error and stops", func() {
				performer1 := pkg.BackupPerformerFunc(
					func(ctx context.Context, date libtime.Date, t libkafka.Topic) (*pkg.BackupResult, error) {
						return nil, context.DeadlineExceeded
					},
				)
				performer2 := pkg.BackupPerformerFunc(
					func(ctx context.Context, date libtime.Date, t libkafka.Topic) (*pkg.BackupResult, error) {
						Fail("should not be called")
						return nil, nil
					},
				)

				list := pkg.BackupPerformerList{performer1, performer2}
				_, err := list.Backup(ctx, backupDate, topic)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("run backup performer"))
			})
		})

		Context("when context is cancelled", func() {
			It("returns context error", func() {
				cancelCtx, cancel := context.WithCancel(ctx)
				cancel()

				performer := pkg.BackupPerformerFunc(
					func(ctx context.Context, date libtime.Date, t libkafka.Topic) (*pkg.BackupResult, error) {
						Fail("should not be called")
						return nil, nil
					},
				)

				list := pkg.BackupPerformerList{performer}
				_, err := list.Backup(cancelCtx, backupDate, topic)
				Expect(err).To(Equal(context.Canceled))
			})
		})

		Context("when performer returns nil result", func() {
			It("handles nil result gracefully", func() {
				performer := pkg.BackupPerformerFunc(
					func(ctx context.Context, date libtime.Date, t libkafka.Topic) (*pkg.BackupResult, error) {
						return nil, nil
					},
				)

				list := pkg.BackupPerformerList{performer}
				result, err := list.Backup(ctx, backupDate, topic)
				Expect(err).NotTo(HaveOccurred())
				Expect(result.MessageCount).To(Equal(int64(0)))
				Expect(result.OffsetCount).To(Equal(int64(0)))
			})
		})
	})
})
