// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"io"

	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
)

// Opener provides file operations for backup storage.
//
//counterfeiter:generate -o ../mocks/opener.go --fake-name Opener . Opener
type Opener interface {
	OpenReader(
		ctx context.Context,
		date libtime.Date,
		topic libkafka.Topic,
		backupType BackupType,
	) (io.ReadCloser, error)
	OpenWriter(
		ctx context.Context,
		date libtime.Date,
		topic libkafka.Topic,
		backupType BackupType,
	) (io.WriteCloser, error)
	List(ctx context.Context) (Dates, error)
}
