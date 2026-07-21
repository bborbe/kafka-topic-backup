// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	"github.com/bborbe/run"
	"github.com/golang/glog"
)

func NewWorker(
	processor TopicProcessor,
	topics <-chan libkafka.Topic,
) run.Func {
	return func(ctx context.Context) error {
		for topic := range topics {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err := processor.Process(ctx, topic); err != nil {
					return errors.Wrapf(ctx, err, "process topic %s failed", topic)
				}
			}
		}
		glog.V(2).Infof("worker finished")
		return nil
	}
}

// NewPartitionWorker creates a worker that processes partition backup tasks.
func NewPartitionWorker(
	processor PartitionProcessor,
	tasks <-chan PartitionTask,
) run.Func {
	return func(ctx context.Context) error {
		for task := range tasks {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err := processor.Process(ctx, task); err != nil {
					return errors.Wrapf(
						ctx,
						err,
						"process partition %s:%d failed",
						task.Topic,
						task.Partition,
					)
				}
			}
		}
		glog.V(2).Infof("partition worker finished")
		return nil
	}
}
