// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	libkafka "github.com/bborbe/kafka"
)

// CorruptRange represents a contiguous range of corrupt offsets in a partition.
type CorruptRange struct {
	Partition      libkafka.Partition
	StartOffset    libkafka.Offset
	EndOffset      libkafka.Offset // Inclusive
	StartTimestamp int64           // Unix milliseconds, 0 if unknown
	EndTimestamp   int64           // Unix milliseconds, 0 if unknown
	ErrorMessage   string
}

// Count returns the number of corrupt messages in this range.
func (c CorruptRange) Count() int64 {
	return int64(c.EndOffset - c.StartOffset + 1)
}

// ScanResult holds the results of scanning a single topic for corruption.
type ScanResult struct {
	Topic           libkafka.Topic
	TotalMessages   int64
	CorruptRanges   []CorruptRange
	FirstOffset     libkafka.Offset
	LastOffset      libkafka.Offset
	ScanDurationSec float64
}

// TotalCorruptMessages returns the total count of corrupt messages across all ranges.
func (s *ScanResult) TotalCorruptMessages() int64 {
	var total int64
	for _, r := range s.CorruptRanges {
		total += r.Count()
	}
	return total
}

// ScanSummary tracks overall progress across multiple topic scans.
//
//counterfeiter:generate -o ../mocks/scan-summary.go --fake-name ScanSummary . ScanSummary
type ScanSummary interface {
	AddCompleted() int32
	AddResult(result *ScanResult)
	Total() int
	Completed() int32
	TotalCorruptRanges() int
	Results() []*ScanResult
}

func NewScanSummary(total int) ScanSummary {
	return &scanSummary{
		total:   total,
		results: make([]*ScanResult, 0, total),
	}
}

type scanSummary struct {
	total     int
	completed atomic.Int32
	results   []*ScanResult
	mux       sync.Mutex
}

func (s *scanSummary) AddCompleted() int32 {
	return s.completed.Add(1)
}

func (s *scanSummary) AddResult(result *ScanResult) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.results = append(s.results, result)
}

func (s *scanSummary) Total() int {
	return s.total
}

func (s *scanSummary) Completed() int32 {
	return s.completed.Load()
}

func (s *scanSummary) TotalCorruptRanges() int {
	s.mux.Lock()
	defer s.mux.Unlock()
	total := 0
	for _, r := range s.results {
		total += len(r.CorruptRanges)
	}
	return total
}

func (s *scanSummary) Results() []*ScanResult {
	s.mux.Lock()
	defer s.mux.Unlock()
	// Return copy to avoid concurrent modification
	results := make([]*ScanResult, len(s.results))
	copy(results, s.results)
	return results
}

// FormatScanResult formats a scan result for console output.
func FormatScanResult(workerID int, result *ScanResult) string {
	output := fmt.Sprintf("[Worker %d] %s\n", workerID, result.Topic)
	output += fmt.Sprintf("  Total messages: %s\n", formatNumber(result.TotalMessages))

	if len(result.CorruptRanges) == 0 {
		output += "  ✓ No corruption found\n"
	} else {
		output += fmt.Sprintf("  ✗ Corrupt ranges: %d\n", len(result.CorruptRanges))
		for _, r := range result.CorruptRanges {
			count := r.Count()
			output += fmt.Sprintf("    Partition %d: offsets %s - %s (%s messages)\n",
				r.Partition,
				formatNumber(int64(r.StartOffset)),
				formatNumber(int64(r.EndOffset)),
				formatNumber(count))

			// Add timestamps if available
			if r.StartTimestamp > 0 {
				output += fmt.Sprintf("      Start timestamp: %s\n", formatTimestamp(r.StartTimestamp))
			}
			if r.EndTimestamp > 0 {
				output += fmt.Sprintf("      End timestamp: %s\n", formatTimestamp(r.EndTimestamp))
			}

			output += fmt.Sprintf("      Error: %s\n", r.ErrorMessage)
		}
	}

	output += fmt.Sprintf("  Scan duration: %.1fs\n", result.ScanDurationSec)
	return output
}

// formatNumber adds thousand separators for readability.
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return fmt.Sprintf("%s,%03d", formatNumber(n/1000), n%1000)
}

// formatTimestamp converts Unix milliseconds to RFC3339 format.
func formatTimestamp(ts int64) string {
	t := time.Unix(ts/1000, (ts%1000)*1000000)
	return t.Format(time.RFC3339)
}
