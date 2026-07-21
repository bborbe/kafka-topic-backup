// Copyright (c) 2025 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/kafka-topic-backup/pkg"
)

var _ = Describe("ConsumerGroupOffsetRestorer", func() {
	Describe("NewConsumerGroupOffsetRestorer", func() {
		It("creates a ConsumerGroupOffsetRestorer", func() {
			restorer := pkg.NewConsumerGroupOffsetRestorer(nil, nil)
			Expect(restorer).NotTo(BeNil())
		})
	})
})
