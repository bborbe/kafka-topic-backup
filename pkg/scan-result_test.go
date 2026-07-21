// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka-topic-backup/pkg"
)

var _ = Describe("FormatScanResult", func() {
	Context("with no corruption", func() {
		It("shows success message", func() {
			result := &pkg.ScanResult{
				Topic:           "test-topic",
				TotalMessages:   1000,
				CorruptRanges:   []pkg.CorruptRange{},
				ScanDurationSec: 1.5,
			}

			output := pkg.FormatScanResult(0, result)

			Expect(output).To(ContainSubstring("test-topic"))
			Expect(output).To(ContainSubstring("1,000"))
			Expect(output).To(ContainSubstring("✓ No corruption found"))
			Expect(output).To(ContainSubstring("1.5s"))
		})
	})

	Context("with corruption", func() {
		It("shows corrupt ranges without timestamps", func() {
			result := &pkg.ScanResult{
				Topic:         "test-topic",
				TotalMessages: 1000,
				CorruptRanges: []pkg.CorruptRange{
					{
						Partition:      0,
						StartOffset:    100,
						EndOffset:      150,
						StartTimestamp: 0, // No timestamp
						EndTimestamp:   0,
						ErrorMessage:   "CRC error",
					},
				},
				ScanDurationSec: 2.3,
			}

			output := pkg.FormatScanResult(0, result)

			Expect(output).To(ContainSubstring("✗ Corrupt ranges: 1"))
			Expect(output).To(ContainSubstring("Partition 0: offsets 100 - 150"))
			Expect(output).To(ContainSubstring("(51 messages)"))
			Expect(output).To(ContainSubstring("Error: CRC error"))
			Expect(output).NotTo(ContainSubstring("timestamp"))
		})

		It("shows corrupt ranges with timestamps", func() {
			// 2025-01-20T12:00:00Z = 1737374400000 ms
			startTs := int64(1737374400000)
			// 2025-01-20T13:00:00Z = 1737378000000 ms
			endTs := int64(1737378000000)

			result := &pkg.ScanResult{
				Topic:         "test-topic",
				TotalMessages: 1000,
				CorruptRanges: []pkg.CorruptRange{
					{
						Partition:      0,
						StartOffset:    100,
						EndOffset:      150,
						StartTimestamp: startTs,
						EndTimestamp:   endTs,
						ErrorMessage:   "CRC mismatch",
					},
				},
				ScanDurationSec: 2.3,
			}

			output := pkg.FormatScanResult(0, result)

			Expect(output).To(ContainSubstring("✗ Corrupt ranges: 1"))
			Expect(output).To(ContainSubstring("Partition 0: offsets 100 - 150"))
			Expect(output).To(ContainSubstring("Start timestamp: 2025-01-20"))
			Expect(output).To(ContainSubstring("End timestamp: 2025-01-20"))
			Expect(output).To(ContainSubstring("Error: CRC mismatch"))
		})

		It("handles multiple corrupt ranges", func() {
			result := &pkg.ScanResult{
				Topic:         "test-topic",
				TotalMessages: 5000,
				CorruptRanges: []pkg.CorruptRange{
					{
						Partition:      0,
						StartOffset:    100,
						EndOffset:      150,
						StartTimestamp: 1737374400000,
						ErrorMessage:   "CRC error 1",
					},
					{
						Partition:    0,
						StartOffset:  500,
						EndOffset:    600,
						EndTimestamp: 1737378000000,
						ErrorMessage: "CRC error 2",
					},
				},
				ScanDurationSec: 5.2,
			}

			output := pkg.FormatScanResult(0, result)

			Expect(output).To(ContainSubstring("✗ Corrupt ranges: 2"))
			Expect(output).To(ContainSubstring("offsets 100 - 150"))
			Expect(output).To(ContainSubstring("offsets 500 - 600"))
			Expect(output).To(ContainSubstring("Start timestamp: 2025-01-20"))
			Expect(output).To(ContainSubstring("End timestamp: 2025-01-20"))
		})
	})

	Context("formatTimestamp", func() {
		It("converts Unix milliseconds to RFC3339", func() {
			// Test a specific known timestamp
			// 2025-01-20T12:34:56.789Z = 1737373496789 ms
			ts := int64(1737373496789)

			// Use the formatTimestamp function via FormatScanResult
			result := &pkg.ScanResult{
				Topic: "test",
				CorruptRanges: []pkg.CorruptRange{
					{
						StartTimestamp: ts,
					},
				},
			}

			output := pkg.FormatScanResult(0, result)

			// Should contain the date portion
			Expect(output).To(ContainSubstring("2025-01-20T"))
			// Should contain the time portion (hour might vary by timezone)
			Expect(output).To(MatchRegexp(`\d{2}:\d{2}:\d{2}`))
		})
	})
})
