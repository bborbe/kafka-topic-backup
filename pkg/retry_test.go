// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka-topic-backup/pkg"
)

var _ = Describe("Retry", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("IsRetryableError", func() {
		It("returns false for nil", func() {
			Expect(pkg.IsRetryableError(nil)).To(BeFalse())
		})

		It("returns true for broken pipe", func() {
			err := errors.New("write tcp 192.168.1.1:1234->192.168.1.2:5678: write: broken pipe")
			Expect(pkg.IsRetryableError(err)).To(BeTrue())
		})

		It("returns false for other errors", func() {
			err := errors.New("some other error")
			Expect(pkg.IsRetryableError(err)).To(BeFalse())
		})
	})

	Describe("RetryWithBackoff", func() {
		It("succeeds on first attempt", func() {
			attempts := 0
			result, err := pkg.RetryWithBackoff(ctx, 3, func() (string, error) {
				attempts++
				return "success", nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal("success"))
			Expect(attempts).To(Equal(1))
		})

		It("retries on retryable error and succeeds", func() {
			attempts := 0
			result, err := pkg.RetryWithBackoff(ctx, 3, func() (string, error) {
				attempts++
				if attempts < 3 {
					return "", errors.New("broken pipe")
				}
				return "success", nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal("success"))
			Expect(attempts).To(Equal(3))
		})

		It("fails immediately on non-retryable error", func() {
			attempts := 0
			_, err := pkg.RetryWithBackoff(ctx, 3, func() (string, error) {
				attempts++
				return "", errors.New("permission denied")
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("permission denied"))
			Expect(attempts).To(Equal(1))
		})

		It("fails after max retries exceeded", func() {
			attempts := 0
			_, err := pkg.RetryWithBackoff(ctx, 2, func() (string, error) {
				attempts++
				return "", errors.New("broken pipe")
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("broken pipe"))
			Expect(attempts).To(Equal(3)) // initial + 2 retries
		})

		It("respects context cancellation", func() {
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			attempts := 0
			_, err := pkg.RetryWithBackoff(ctx, 3, func() (string, error) {
				attempts++
				return "", errors.New("broken pipe")
			})
			Expect(err).To(Equal(context.Canceled))
			Expect(attempts).To(Equal(1))
		})
	})

	Describe("RetryVoidWithBackoff", func() {
		It("succeeds on first attempt", func() {
			attempts := 0
			err := pkg.RetryVoidWithBackoff(ctx, 3, func() error {
				attempts++
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(attempts).To(Equal(1))
		})

		It("retries on retryable error and succeeds", func() {
			attempts := 0
			err := pkg.RetryVoidWithBackoff(ctx, 3, func() error {
				attempts++
				if attempts < 2 {
					return errors.New("broken pipe")
				}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(attempts).To(Equal(2))
		})
	})
})
