# Changelog

All notable changes to this project will be documented in this file.

Please choose versions by [Semantic Versioning](http://semver.org/).

* MAJOR version when you make incompatible API changes,
* MINOR version when you add functionality in a backwards compatible manner, and
* PATCH version when you make backwards compatible bug fixes.

## Unreleased

- chore: update github.com/IBM/sarama to v1.60.2, github.com/bborbe/errors to v1.5.21, github.com/bborbe/http to v1.26.24, github.com/bborbe/log to v1.6.25, github.com/bborbe/metrics to v0.5.15, github.com/bborbe/sentry to v1.9.27

## v0.3.0

- feat: opt into `autoMerge.trivial` for mechanically-trivial update PRs

## v0.2.0

- feat: add self-contained Makefiles for the cmd/ operator tools (backup, cleaner, restore, scanner)
- fix: define DATADIR for cmd/cleaner and cmd/restore, which previously ran with -datadir=""
- fix: refuse `make backup` when the topic list resolves empty, instead of silently backing up nothing
- fix: `make verify-backups` now fails when no partitions are found, instead of reporting "0/0 complete"
- fix: guard jq-dependent targets with a clear error when jq is absent
- fix: declare every cmd/ target .PHONY, so a stray file of the same name cannot silently no-op it

## v0.1.4

- chore: update Go to 1.27.0 and update dependencies

## v0.1.3

- chore: Bump errcheck to v1.20.0 and golangci-lint to v2.13.1 for Go 1.27 support
## v0.1.2

- update Go to 1.26.6 and update dependencies (fixes GO-2026-6179, GO-2026-6180)

## v0.1.1

- docs: add a License section to the README

## v0.1.0

- Initial release — extracted from `bborbe/trading` (`strimzi/topic-backuper`) as a standalone public repo
- Kafka topic backup/restore/scan/clean toolkit: Avro-format, incremental, corruption-tolerant, per-partition; broker-agnostic
- Root HTTP service + `cmd/{backup,restore,scanner,cleaner}` CLIs
- Decoupled from `trading/lib`: build-info via public `github.com/bborbe/metrics`, loglevel + sync-producer via public `github.com/bborbe/log` / `github.com/bborbe/kafka`
- Publish-only build → `docker.io/bborbe/kafka-topic-backup:vX.Y.Z`
