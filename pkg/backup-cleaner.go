// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"os"
	"path"
	"sort"

	"github.com/bborbe/errors"
	libkafka "github.com/bborbe/kafka"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"
)

func NewBackupCleaner(
	rootDir string,
	retainCount int,
	dryRun bool,
) *BackupCleaner {
	return &BackupCleaner{
		rootDir:     rootDir,
		retainCount: retainCount,
		dryRun:      dryRun,
	}
}

type BackupCleaner struct {
	rootDir     string
	retainCount int
	dryRun      bool
}

type topicBackup struct {
	Date  libtime.Date
	Topic libkafka.Topic
	Path  string
}

func (c *BackupCleaner) Clean(ctx context.Context) (int, error) {
	opener := NewFileOpener(c.rootDir)

	// List all backup dates
	dates, err := opener.List(ctx)
	if err != nil {
		return 0, errors.Wrap(ctx, err, "list backup dates failed")
	}

	if len(dates) == 0 {
		glog.V(1).Info("no backups found")
		return 0, nil
	}

	// Build map of topic -> list of successful backups
	topicBackups := make(map[libkafka.Topic][]topicBackup)

	for _, date := range dates {
		dateDir := path.Join(c.rootDir, date.Format(DateLayout))
		entries, err := os.ReadDir(dateDir)
		if err != nil {
			glog.Warningf("read date dir %s failed: %v", dateDir, err)
			continue
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}

			topic := libkafka.Topic(entry.Name())
			backupPath := path.Join(dateDir, entry.Name())

			// Check if backup was successful
			stats, err := opener.ReadStats(ctx, date, topic)
			if err != nil {
				glog.V(2).
					Infof("skip %s/%s: read stats failed: %v", date.Format(DateLayout), topic, err)
				continue
			}
			if !stats.Success {
				glog.V(2).Infof("skip %s/%s: backup not successful", date.Format(DateLayout), topic)
				continue
			}

			topicBackups[topic] = append(topicBackups[topic], topicBackup{
				Date:  date,
				Topic: topic,
				Path:  backupPath,
			})
		}
	}

	// For each topic, keep only the last N successful backups
	deleted := 0
	for topic, backups := range topicBackups {
		// Sort by date descending (newest first)
		sort.Slice(backups, func(i, j int) bool {
			return backups[i].Date.Time().After(backups[j].Date.Time())
		})

		if len(backups) <= c.retainCount {
			glog.V(2).Infof("topic %s: %d backups, keeping all", topic, len(backups))
			continue
		}

		// Delete backups beyond retainCount
		toDelete := backups[c.retainCount:]
		glog.V(1).
			Infof("topic %s: %d backups, deleting %d oldest", topic, len(backups), len(toDelete))

		for _, backup := range toDelete {
			if c.dryRun {
				glog.Infof("Would delete: %s", backup.Path)
			} else {
				glog.V(2).Infof("Deleting: %s", backup.Path)
				if err := os.RemoveAll(backup.Path); err != nil {
					return deleted, errors.Wrapf(ctx, err, "delete %s failed", backup.Path)
				}
			}
			deleted++
		}
	}

	// Clean up empty date directories
	for _, date := range dates {
		dateDir := path.Join(c.rootDir, date.Format(DateLayout))
		entries, err := os.ReadDir(dateDir)
		if err != nil {
			continue
		}
		if len(entries) == 0 {
			if c.dryRun {
				glog.Infof("Would delete empty dir: %s", dateDir)
			} else {
				glog.V(2).Infof("Deleting empty dir: %s", dateDir)
				if err := os.Remove(dateDir); err != nil {
					glog.Warningf("delete empty dir %s failed: %v", dateDir, err)
				}
			}
		}
	}

	return deleted, nil
}
