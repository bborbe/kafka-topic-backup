// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"os"
	"path/filepath"
	"time"

	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka-topic-backup/pkg"
)

var _ = Describe("BackupCleaner", func() {
	var (
		ctx     context.Context
		tempDir string
		opener  *pkg.FileOpener
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		tempDir, err = os.MkdirTemp("", "backup-cleaner-test-*")
		Expect(err).NotTo(HaveOccurred())

		opener = pkg.NewFileOpener(tempDir)
	})

	AfterEach(func() {
		if tempDir != "" {
			_ = os.RemoveAll(tempDir)
		}
	})

	Describe("NewBackupCleaner", func() {
		It("creates a BackupCleaner", func() {
			cleaner := pkg.NewBackupCleaner(tempDir, 7, false)
			Expect(cleaner).NotTo(BeNil())
		})
	})

	Describe("Clean", func() {
		Context("with empty directory", func() {
			It("returns zero deleted", func() {
				cleaner := pkg.NewBackupCleaner(tempDir, 7, false)
				deleted, err := cleaner.Clean(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(deleted).To(Equal(0))
			})
		})

		Context("with fewer backups than retain count", func() {
			BeforeEach(func() {
				// Create 3 successful backups for a topic
				dates := []libtime.Date{
					libtime.Date(time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)),
					libtime.Date(time.Date(2025, 1, 14, 0, 0, 0, 0, time.UTC)),
					libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)),
				}
				topic := libkafka.Topic("test-topic")

				for _, date := range dates {
					writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
					Expect(err).NotTo(HaveOccurred())
					writer.Close()

					err = opener.WriteStats(ctx, date, topic, pkg.BackupStats{
						Topic:   topic.String(),
						Success: true,
					})
					Expect(err).NotTo(HaveOccurred())
				}
			})

			It("keeps all backups when count <= retain", func() {
				cleaner := pkg.NewBackupCleaner(tempDir, 7, false) // retain 7, have 3
				deleted, err := cleaner.Clean(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(deleted).To(Equal(0))

				// Verify all backups still exist
				for _, date := range []string{"2025-01-13", "2025-01-14", "2025-01-15"} {
					path := filepath.Join(tempDir, date, "test-topic")
					_, err := os.Stat(path)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		Context("with more backups than retain count", func() {
			BeforeEach(func() {
				// Create 5 successful backups for a topic
				dates := []libtime.Date{
					libtime.Date(time.Date(2025, 1, 11, 0, 0, 0, 0, time.UTC)),
					libtime.Date(time.Date(2025, 1, 12, 0, 0, 0, 0, time.UTC)),
					libtime.Date(time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)),
					libtime.Date(time.Date(2025, 1, 14, 0, 0, 0, 0, time.UTC)),
					libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)),
				}
				topic := libkafka.Topic("test-topic")

				for _, date := range dates {
					writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
					Expect(err).NotTo(HaveOccurred())
					writer.Close()

					err = opener.WriteStats(ctx, date, topic, pkg.BackupStats{
						Topic:   topic.String(),
						Success: true,
					})
					Expect(err).NotTo(HaveOccurred())
				}
			})

			It("deletes oldest backups beyond retain count", func() {
				cleaner := pkg.NewBackupCleaner(tempDir, 3, false) // retain 3, have 5
				deleted, err := cleaner.Clean(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(deleted).To(Equal(2)) // 5 - 3 = 2 deleted

				// Verify oldest backups deleted
				for _, date := range []string{"2025-01-11", "2025-01-12"} {
					path := filepath.Join(tempDir, date, "test-topic")
					_, err := os.Stat(path)
					Expect(os.IsNotExist(err)).To(BeTrue())
				}

				// Verify newest backups kept
				for _, date := range []string{"2025-01-13", "2025-01-14", "2025-01-15"} {
					path := filepath.Join(tempDir, date, "test-topic")
					_, err := os.Stat(path)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		Context("with dry run enabled", func() {
			BeforeEach(func() {
				// Create 3 successful backups
				dates := []libtime.Date{
					libtime.Date(time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)),
					libtime.Date(time.Date(2025, 1, 14, 0, 0, 0, 0, time.UTC)),
					libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)),
				}
				topic := libkafka.Topic("test-topic")

				for _, date := range dates {
					writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
					Expect(err).NotTo(HaveOccurred())
					writer.Close()

					err = opener.WriteStats(ctx, date, topic, pkg.BackupStats{
						Topic:   topic.String(),
						Success: true,
					})
					Expect(err).NotTo(HaveOccurred())
				}
			})

			It("reports deletions without actually deleting", func() {
				cleaner := pkg.NewBackupCleaner(tempDir, 1, true) // retain 1, have 3, dry run
				deleted, err := cleaner.Clean(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(deleted).To(Equal(2)) // would delete 2

				// Verify all backups still exist (dry run)
				for _, date := range []string{"2025-01-13", "2025-01-14", "2025-01-15"} {
					path := filepath.Join(tempDir, date, "test-topic")
					_, err := os.Stat(path)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		Context("with unsuccessful backups", func() {
			BeforeEach(func() {
				// Create mix of successful and unsuccessful backups
				topic := libkafka.Topic("test-topic")

				// Successful backup
				date1 := libtime.Date(time.Date(2025, 1, 14, 0, 0, 0, 0, time.UTC))
				writer, err := opener.OpenWriter(ctx, date1, topic, pkg.BackupTypeTopic)
				Expect(err).NotTo(HaveOccurred())
				writer.Close()
				err = opener.WriteStats(ctx, date1, topic, pkg.BackupStats{
					Topic:   topic.String(),
					Success: true,
				})
				Expect(err).NotTo(HaveOccurred())

				// Unsuccessful backup
				date2 := libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
				writer, err = opener.OpenWriter(ctx, date2, topic, pkg.BackupTypeTopic)
				Expect(err).NotTo(HaveOccurred())
				writer.Close()
				err = opener.WriteStats(ctx, date2, topic, pkg.BackupStats{
					Topic:   topic.String(),
					Success: false,
					Error:   "backup failed",
				})
				Expect(err).NotTo(HaveOccurred())
			})

			It("only counts successful backups for retention", func() {
				cleaner := pkg.NewBackupCleaner(tempDir, 1, false)
				deleted, err := cleaner.Clean(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(deleted).To(Equal(0)) // Only 1 successful backup, retain 1
			})
		})

		Context("with multiple topics", func() {
			BeforeEach(func() {
				// Create 3 backups each for 2 topics
				dates := []libtime.Date{
					libtime.Date(time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)),
					libtime.Date(time.Date(2025, 1, 14, 0, 0, 0, 0, time.UTC)),
					libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)),
				}
				topics := []libkafka.Topic{"topic-a", "topic-b"}

				for _, topic := range topics {
					for _, date := range dates {
						writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
						Expect(err).NotTo(HaveOccurred())
						writer.Close()

						err = opener.WriteStats(ctx, date, topic, pkg.BackupStats{
							Topic:   topic.String(),
							Success: true,
						})
						Expect(err).NotTo(HaveOccurred())
					}
				}
			})

			It("applies retention per topic independently", func() {
				cleaner := pkg.NewBackupCleaner(tempDir, 2, false) // retain 2 per topic
				deleted, err := cleaner.Clean(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(deleted).To(Equal(2)) // 1 per topic * 2 topics

				// Verify oldest backup deleted for each topic
				for _, topic := range []string{"topic-a", "topic-b"} {
					path := filepath.Join(tempDir, "2025-01-13", topic)
					_, err := os.Stat(path)
					Expect(os.IsNotExist(err)).To(BeTrue())
				}
			})
		})
	})
})
