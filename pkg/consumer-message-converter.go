// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"time"

	"github.com/IBM/sarama"

	"github.com/bborbe/kafka-topic-backup/avro"
)

//counterfeiter:generate -o ../../mocks/consumer-message-converter.go --fake-name ConsumerMessageConverter . ConsumerMessageConverter
type ConsumerMessageConverter interface {
	Convert(msg *sarama.ConsumerMessage) avro.BackupTopicRecord
}

func NewConsumerMessageConverter() ConsumerMessageConverter {
	return &consumerMessageConverter{}
}

type consumerMessageConverter struct {
}

func (c *consumerMessageConverter) Convert(msg *sarama.ConsumerMessage) avro.BackupTopicRecord {
	headers := make([]avro.BackupTopicRecordHeader, 0)
	for _, h := range msg.Headers {
		headers = append(headers, avro.BackupTopicRecordHeader{
			Key:   h.Key,
			Value: h.Value,
		})
	}
	return avro.BackupTopicRecord{
		Key:            msg.Key,
		Value:          msg.Value,
		Headers:        headers,
		Timestamp:      timeToTimestamp(msg.Timestamp),
		BlockTimestamp: timeToTimestamp(msg.BlockTimestamp),
		Topic:          msg.Topic,
		Partition:      msg.Partition,
		Offset:         msg.Offset,
	}
}

func timeToTimestamp(t time.Time) int64 {
	return t.UnixNano()
}
