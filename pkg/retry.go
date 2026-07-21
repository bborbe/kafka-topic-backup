// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"time"

	"github.com/golang/glog"
)

const (
	DefaultMaxRetries   = 3
	DefaultInitialDelay = 100 * time.Millisecond
	DefaultMaxDelay     = 5 * time.Second
)

// IsRetryableError checks if an error is transient and worth retrying.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	return IsBrokenPipeError(err)
}

// RetryWithBackoff executes fn with exponential backoff for retryable errors.
func RetryWithBackoff[T any](
	ctx context.Context,
	maxRetries int,
	fn func() (T, error),
) (T, error) {
	var zero T
	var lastErr error
	delay := DefaultInitialDelay

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			glog.V(2).Infof("retry attempt %d/%d after %v", attempt, maxRetries, delay)
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(delay):
			}
			// Exponential backoff with cap
			delay = delay * 2
			if delay > DefaultMaxDelay {
				delay = DefaultMaxDelay
			}
		}

		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err
		if !IsRetryableError(err) {
			return zero, err
		}
		glog.V(1).Infof("retryable error (attempt %d/%d): %v", attempt+1, maxRetries+1, err)
	}

	return zero, lastErr
}

// RetryVoidWithBackoff executes fn with exponential backoff for retryable errors.
func RetryVoidWithBackoff(
	ctx context.Context,
	maxRetries int,
	fn func() error,
) error {
	_, err := RetryWithBackoff(ctx, maxRetries, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}
