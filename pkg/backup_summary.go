// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"sync"
	"sync/atomic"

	libkafka "github.com/bborbe/kafka"
)

// BackupSummary tracks backup progress across workers in a thread-safe manner.
//
//counterfeiter:generate -o ../mocks/backup-summary.go --fake-name BackupSummary . BackupSummary
type BackupSummary interface {
	AddCompleted() int32
	AddSkipped()
	AddFailed(topic libkafka.Topic)
	AddCRCErrors(topic libkafka.Topic, partition libkafka.Partition, rangeCount int)
	Total() int
	Completed() int32
	Skipped() int32
	Failed() int32
	Succeeded() int32
	FailedTopics() []libkafka.Topic
	CRCErrors() map[libkafka.Topic]map[libkafka.Partition]int
	CRCTopicCount() int
}

// NewBackupSummary creates a new BackupSummary.
func NewBackupSummary(totalTopics int) BackupSummary {
	return &backupSummary{
		totalTopics:  totalTopics,
		failedTopics: make([]libkafka.Topic, 0),
		crcErrors:    make(map[libkafka.Topic]map[libkafka.Partition]int),
	}
}

type backupSummary struct {
	totalTopics  int
	completed    atomic.Int32
	skipped      atomic.Int32
	failed       atomic.Int32
	failedTopics []libkafka.Topic
	failedMutex  sync.Mutex
	crcErrors    map[libkafka.Topic]map[libkafka.Partition]int
	crcMutex     sync.Mutex
}

// AddCompleted increments the completed counter and returns the current count.
func (s *backupSummary) AddCompleted() int32 {
	return s.completed.Add(1)
}

// AddSkipped increments the skipped counter.
func (s *backupSummary) AddSkipped() {
	s.skipped.Add(1)
}

// AddFailed increments the failed counter and records the topic name.
func (s *backupSummary) AddFailed(topic libkafka.Topic) {
	s.failed.Add(1)
	s.failedMutex.Lock()
	s.failedTopics = append(s.failedTopics, topic)
	s.failedMutex.Unlock()
}

// Total returns the total number of topics.
func (s *backupSummary) Total() int {
	return s.totalTopics
}

// Completed returns the completed count.
func (s *backupSummary) Completed() int32 {
	return s.completed.Load()
}

// Skipped returns the skipped count.
func (s *backupSummary) Skipped() int32 {
	return s.skipped.Load()
}

// Failed returns the failed count.
func (s *backupSummary) Failed() int32 {
	return s.failed.Load()
}

// Succeeded returns the succeeded count (completed - skipped - failed).
func (s *backupSummary) Succeeded() int32 {
	return s.Completed() - s.Skipped() - s.Failed()
}

// FailedTopics returns a copy of the failed topics list.
func (s *backupSummary) FailedTopics() []libkafka.Topic {
	s.failedMutex.Lock()
	defer s.failedMutex.Unlock()

	result := make([]libkafka.Topic, len(s.failedTopics))
	copy(result, s.failedTopics)
	return result
}

// AddCRCErrors records CRC errors (skipped ranges) for a topic/partition.
func (s *backupSummary) AddCRCErrors(
	topic libkafka.Topic,
	partition libkafka.Partition,
	rangeCount int,
) {
	s.crcMutex.Lock()
	defer s.crcMutex.Unlock()

	if _, exists := s.crcErrors[topic]; !exists {
		s.crcErrors[topic] = make(map[libkafka.Partition]int)
	}
	s.crcErrors[topic][partition] = rangeCount
}

// CRCErrors returns a copy of the CRC error map.
func (s *backupSummary) CRCErrors() map[libkafka.Topic]map[libkafka.Partition]int {
	s.crcMutex.Lock()
	defer s.crcMutex.Unlock()

	result := make(map[libkafka.Topic]map[libkafka.Partition]int)
	for topic, partitions := range s.crcErrors {
		result[topic] = make(map[libkafka.Partition]int)
		for partition, count := range partitions {
			result[topic][partition] = count
		}
	}
	return result
}

// CRCTopicCount returns the number of topics with CRC errors.
func (s *backupSummary) CRCTopicCount() int {
	s.crcMutex.Lock()
	defer s.crcMutex.Unlock()
	return len(s.crcErrors)
}
