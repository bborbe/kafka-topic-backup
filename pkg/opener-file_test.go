// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka-topic-backup/pkg"
)

var _ = Describe("FileOpener", func() {
	var (
		ctx     context.Context
		tempDir string
		opener  *pkg.FileOpener
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		tempDir, err = os.MkdirTemp("", "file-opener-test-*")
		Expect(err).NotTo(HaveOccurred())

		opener = pkg.NewFileOpener(tempDir)
	})

	AfterEach(func() {
		if tempDir != "" {
			_ = os.RemoveAll(tempDir)
		}
	})

	Describe("NewFileOpener", func() {
		It("creates a FileOpener", func() {
			Expect(opener).NotTo(BeNil())
		})
	})

	Describe("OpenWriter", func() {
		It("creates directory structure and returns writer", func() {
			date := libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
			topic := libkafka.Topic("test-topic")

			writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
			Expect(err).NotTo(HaveOccurred())
			defer writer.Close()

			// Write some data
			_, err = writer.Write([]byte("test data"))
			Expect(err).NotTo(HaveOccurred())

			// Verify directory was created
			expectedDir := filepath.Join(tempDir, "2025-01-15", "test-topic")
			_, err = os.Stat(expectedDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify file exists
			expectedFile := filepath.Join(expectedDir, "topic.avro")
			_, err = os.Stat(expectedFile)
			Expect(err).NotTo(HaveOccurred())
		})

		It("handles offset backup type", func() {
			date := libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
			topic := libkafka.Topic("test-topic")

			writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeOffset)
			Expect(err).NotTo(HaveOccurred())
			writer.Close()

			// Verify correct file name
			expectedFile := filepath.Join(tempDir, "2025-01-15", "test-topic", "offset.avro")
			_, err = os.Stat(expectedFile)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("OpenReader", func() {
		It("reads existing file", func() {
			date := libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
			topic := libkafka.Topic("test-topic")

			// First write some data
			writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
			Expect(err).NotTo(HaveOccurred())
			_, err = writer.Write([]byte("test content"))
			Expect(err).NotTo(HaveOccurred())
			writer.Close()

			// Now read it back
			reader, err := opener.OpenReader(ctx, date, topic, pkg.BackupTypeTopic)
			Expect(err).NotTo(HaveOccurred())
			defer reader.Close()

			content, err := io.ReadAll(reader)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(content)).To(Equal("test content"))
		})

		It("returns error for non-existent file", func() {
			date := libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
			topic := libkafka.Topic("non-existent-topic")

			_, err := opener.OpenReader(ctx, date, topic, pkg.BackupTypeTopic)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("List", func() {
		It("returns empty list for empty directory", func() {
			dates, err := opener.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(dates).To(BeEmpty())
		})

		It("returns dates from directory names", func() {
			// Create some date directories
			Expect(os.MkdirAll(filepath.Join(tempDir, "2025-01-15"), 0750)).To(Succeed())
			Expect(os.MkdirAll(filepath.Join(tempDir, "2025-01-16"), 0750)).To(Succeed())
			Expect(
				os.MkdirAll(filepath.Join(tempDir, "invalid-date"), 0750),
			).To(Succeed())
			// Should be ignored

			dates, err := opener.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(dates).To(HaveLen(2))
		})
	})

	Describe("Stats operations", func() {
		var (
			date  libtime.Date
			topic libkafka.Topic
		)

		BeforeEach(func() {
			date = libtime.Date(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
			topic = libkafka.Topic("test-topic")

			// Create directory structure
			writer, err := opener.OpenWriter(ctx, date, topic, pkg.BackupTypeTopic)
			Expect(err).NotTo(HaveOccurred())
			writer.Close()
		})

		Describe("WriteStats and ReadStats", func() {
			It("writes and reads stats correctly", func() {
				stats := pkg.BackupStats{
					Topic:           "test-topic",
					StartedAt:       time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
					CompletedAt:     time.Date(2025, 1, 15, 10, 5, 0, 0, time.UTC),
					DurationSeconds: 300.0,
					MessageCount:    1000,
					OffsetCount:     10,
					Success:         true,
				}

				err := opener.WriteStats(ctx, date, topic, stats)
				Expect(err).NotTo(HaveOccurred())

				readStats, err := opener.ReadStats(ctx, date, topic)
				Expect(err).NotTo(HaveOccurred())
				Expect(readStats.Topic).To(Equal("test-topic"))
				Expect(readStats.MessageCount).To(Equal(int64(1000)))
				Expect(readStats.OffsetCount).To(Equal(int64(10)))
				Expect(readStats.Success).To(BeTrue())
				Expect(readStats.DurationSeconds).To(Equal(300.0))
			})
		})

		Describe("StatsExists", func() {
			It("returns false when stats don't exist", func() {
				exists, err := opener.StatsExists(ctx, date, topic)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})

			It("returns true when stats exist", func() {
				stats := pkg.BackupStats{
					Topic:   "test-topic",
					Success: true,
				}
				err := opener.WriteStats(ctx, date, topic, stats)
				Expect(err).NotTo(HaveOccurred())

				exists, err := opener.StatsExists(ctx, date, topic)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
			})
		})
	})

	Describe("Partition Stats operations (incremental backup)", func() {
		var (
			topic     libkafka.Topic
			partition libkafka.Partition
		)

		BeforeEach(func() {
			topic = libkafka.Topic("test-topic")
			partition = libkafka.Partition(0)

			// Create directory structure (like backup does via OpenWriterPartition)
			writer, err := opener.OpenWriterPartition(
				ctx,
				topic,
				partition,
				pkg.BackupTypeTopic,
				false,
			)
			Expect(err).NotTo(HaveOccurred())
			writer.Close()
		})

		Describe("WriteStatsPartition and ReadStatsPartition", func() {
			It("writes and reads partition stats for incremental resume", func() {
				stats := pkg.BackupStats{
					Topic:        "test-topic",
					Partition:    0,
					FirstOffset:  100,
					LastOffset:   500,
					MessageCount: 401,
					Success:      true,
				}

				err := opener.WriteStatsPartition(ctx, topic, partition, stats)
				Expect(err).NotTo(HaveOccurred())

				readStats, err := opener.ReadStatsPartition(ctx, topic, partition)
				Expect(err).NotTo(HaveOccurred())
				Expect(readStats.FirstOffset).To(Equal(libkafka.Offset(100)))
				Expect(readStats.LastOffset).To(Equal(libkafka.Offset(500)))
				Expect(readStats.MessageCount).To(Equal(int64(401)))
				Expect(readStats.Success).To(BeTrue())
			})

			It("returns error when no stats exist (fresh backup)", func() {
				_, err := opener.ReadStatsPartition(ctx, topic, partition)
				Expect(err).To(HaveOccurred())
			})

			It("preserves all fields needed for incremental resume", func() {
				stats := pkg.BackupStats{
					Topic:         "test-topic",
					Partition:     0,
					FirstOffset:   0,
					LastOffset:    999,
					MessageCount:  1000,
					OffsetCount:   1000,
					Success:       true,
					SkippedRanges: []pkg.OffsetRange{{Start: 50, End: 55}},
				}

				err := opener.WriteStatsPartition(ctx, topic, partition, stats)
				Expect(err).NotTo(HaveOccurred())

				readStats, err := opener.ReadStatsPartition(ctx, topic, partition)
				Expect(err).NotTo(HaveOccurred())
				Expect(readStats.LastOffset).To(Equal(libkafka.Offset(999)))
				Expect(readStats.SkippedRanges).To(HaveLen(1))
				Expect(readStats.SkippedRanges[0].Start).To(Equal(int64(50)))
			})
		})

		Describe("StatsExistsPartition", func() {
			It("returns false when no stats exist", func() {
				exists, err := opener.StatsExistsPartition(ctx, topic, partition)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})

			It("returns true when stats exist", func() {
				stats := pkg.BackupStats{
					Topic:     "test-topic",
					Partition: 0,
					Success:   true,
				}
				err := opener.WriteStatsPartition(ctx, topic, partition, stats)
				Expect(err).NotTo(HaveOccurred())

				exists, err := opener.StatsExistsPartition(ctx, topic, partition)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
			})
		})

		Describe("Incremental resume calculation", func() {
			It("calculates correct resume offset from last successful backup", func() {
				stats := pkg.BackupStats{
					Topic:        "test-topic",
					Partition:    0,
					FirstOffset:  0,
					LastOffset:   999,
					MessageCount: 1000,
					Success:      true,
				}

				err := opener.WriteStatsPartition(ctx, topic, partition, stats)
				Expect(err).NotTo(HaveOccurred())

				readStats, err := opener.ReadStatsPartition(ctx, topic, partition)
				Expect(err).NotTo(HaveOccurred())

				// Resume offset should be LastOffset + 1
				resumeOffset := readStats.LastOffset + 1
				Expect(resumeOffset).To(Equal(libkafka.Offset(1000)))
			})

			It("does not resume from failed backup", func() {
				stats := pkg.BackupStats{
					Topic:        "test-topic",
					Partition:    0,
					FirstOffset:  0,
					LastOffset:   500,
					MessageCount: 500,
					Success:      false, // Failed backup
					Error:        "connection lost",
				}

				err := opener.WriteStatsPartition(ctx, topic, partition, stats)
				Expect(err).NotTo(HaveOccurred())

				readStats, err := opener.ReadStatsPartition(ctx, topic, partition)
				Expect(err).NotTo(HaveOccurred())

				// Should NOT resume from failed backup
				Expect(readStats.Success).To(BeFalse())
			})
		})
	})
})
