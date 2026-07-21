// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"strings"
)

// IsCorruptionError checks if an error is related to CRC corruption or corrupt records.
// These errors indicate data integrity issues that can be skipped during scanning.
func IsCorruptionError(err error) bool {
	if err == nil {
		return false
	}

	// Check error message for common corruption indicators
	errStr := err.Error()
	return strings.Contains(errStr, "CRC") ||
		strings.Contains(errStr, "CorruptRecord") ||
		strings.Contains(errStr, "message contents does not match") ||
		strings.Contains(errStr, "crc32 field does not match") ||
		strings.Contains(errStr, "invalid CRC")
}
