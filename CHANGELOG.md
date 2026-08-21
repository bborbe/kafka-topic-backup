# Changelog

All notable changes to this project will be documented in this file.

Please choose versions by [Semantic Versioning](http://semver.org/).

* MAJOR version when you make incompatible API changes,
* MINOR version when you add functionality in a backwards compatible manner, and
* PATCH version when you make backwards compatible bug fixes.

## Unreleased

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
