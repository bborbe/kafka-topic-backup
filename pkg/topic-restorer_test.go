// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"bytes"
	"io"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka-topic-backup/avro"
	"github.com/bborbe/kafka-topic-backup/pkg"
)

var _ = Describe("TopicRestorer", func() {
	Describe("NewTopicRestorer", func() {
		It("creates a TopicRestorer", func() {
			restorer := pkg.NewTopicRestorer(nil, nil)
			Expect(restorer).NotTo(BeNil())
		})
	})
})

var _ = Describe("AVRO Serialization", func() {
	Describe("BackupTopicRecord", func() {
		It("serializes and deserializes correctly", func() {
			original := avro.BackupTopicRecord{
				Key:            []byte("test-key"),
				Value:          []byte("test-value"),
				Headers:        []avro.BackupTopicRecordHeader{},
				Timestamp:      1735560000000, // 2024-12-30 12:00:00 UTC in millis
				BlockTimestamp: 1735560000000,
				Topic:          "test-topic",
				Partition:      0,
				Offset:         42,
			}

			// Serialize
			var buf bytes.Buffer
			err := original.Serialize(&buf)
			Expect(err).NotTo(HaveOccurred())

			// Deserialize
			restored, err := avro.DeserializeBackupTopicRecord(&buf)
			Expect(err).NotTo(HaveOccurred())

			Expect(restored.Key).To(Equal(original.Key))
			Expect(restored.Value).To(Equal(original.Value))
			Expect(restored.Timestamp).To(Equal(original.Timestamp))
			Expect(restored.Topic).To(Equal(original.Topic))
			Expect(restored.Partition).To(Equal(original.Partition))
			Expect(restored.Offset).To(Equal(original.Offset))
		})

		It("handles headers", func() {
			original := avro.BackupTopicRecord{
				Key:   []byte("key"),
				Value: []byte("value"),
				Headers: []avro.BackupTopicRecordHeader{
					{Key: []byte("header-key"), Value: []byte("header-value")},
				},
				Timestamp:      1735560000000,
				BlockTimestamp: 1735560000000,
				Topic:          "topic",
				Partition:      1,
				Offset:         100,
			}

			var buf bytes.Buffer
			err := original.Serialize(&buf)
			Expect(err).NotTo(HaveOccurred())

			restored, err := avro.DeserializeBackupTopicRecord(&buf)
			Expect(err).NotTo(HaveOccurred())
			Expect(restored.Headers).To(HaveLen(1))
			Expect([]byte(restored.Headers[0].Key)).To(Equal([]byte("header-key")))
			Expect([]byte(restored.Headers[0].Value)).To(Equal([]byte("header-value")))
		})

		It("handles multiple records", func() {
			var buf bytes.Buffer

			// Write multiple records
			for i := int64(0); i < 3; i++ {
				record := avro.BackupTopicRecord{
					Key:            []byte("key"),
					Value:          []byte("value"),
					Headers:        []avro.BackupTopicRecordHeader{},
					Timestamp:      1735560000000,
					BlockTimestamp: 1735560000000,
					Topic:          "topic",
					Partition:      0,
					Offset:         i,
				}
				err := record.Serialize(&buf)
				Expect(err).NotTo(HaveOccurred())
			}

			// Read multiple records
			reader := bytes.NewReader(buf.Bytes())
			for i := int64(0); i < 3; i++ {
				record, err := avro.DeserializeBackupTopicRecord(reader)
				Expect(err).NotTo(HaveOccurred())
				Expect(record.Offset).To(Equal(i))
			}

			// EOF on next read
			_, err := avro.DeserializeBackupTopicRecord(reader)
			Expect(err).To(Equal(io.EOF))
		})
	})

	Describe("BackupTopicConsumerGroupOffset", func() {
		It("serializes and deserializes correctly", func() {
			original := avro.BackupTopicConsumerGroupOffset{
				Topic:         "test-topic",
				ConsumerGroup: "test-consumer-group",
				Partition:     2,
				Offset:        12345,
			}

			var buf bytes.Buffer
			err := original.Serialize(&buf)
			Expect(err).NotTo(HaveOccurred())

			restored, err := avro.DeserializeBackupTopicConsumerGroupOffset(&buf)
			Expect(err).NotTo(HaveOccurred())

			Expect(restored.Topic).To(Equal(original.Topic))
			Expect(restored.ConsumerGroup).To(Equal(original.ConsumerGroup))
			Expect(restored.Partition).To(Equal(original.Partition))
			Expect(restored.Offset).To(Equal(original.Offset))
		})

		It("handles multiple offsets", func() {
			var buf bytes.Buffer

			// Write multiple offset records
			for i := int32(0); i < 3; i++ {
				record := avro.BackupTopicConsumerGroupOffset{
					Topic:         "topic",
					ConsumerGroup: "group",
					Partition:     i,
					Offset:        int64(i * 100),
				}
				err := record.Serialize(&buf)
				Expect(err).NotTo(HaveOccurred())
			}

			// Read multiple records
			reader := bytes.NewReader(buf.Bytes())
			for i := int32(0); i < 3; i++ {
				record, err := avro.DeserializeBackupTopicConsumerGroupOffset(reader)
				Expect(err).NotTo(HaveOccurred())
				Expect(record.Partition).To(Equal(i))
				Expect(record.Offset).To(Equal(int64(i * 100)))
			}

			// EOF on next read
			_, err := avro.DeserializeBackupTopicConsumerGroupOffset(reader)
			Expect(err).To(Equal(io.EOF))
		})
	})
})
