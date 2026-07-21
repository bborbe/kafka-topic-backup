# Changelog

All notable changes to this project will be documented in this file.

Please choose versions by [Semantic Versioning](http://semver.org/).

* MAJOR version when you make incompatible API changes,
* MINOR version when you add functionality in a backwards compatible manner, and
* PATCH version when you make backwards compatible bug fixes.

## v0.1.0

- Initial release — extracted from `bborbe/trading` (`strimzi/topic-backuper`) as a standalone public repo
- Kafka topic backup/restore/scan/clean toolkit: Avro-format, incremental, corruption-tolerant, per-partition; broker-agnostic
- Root HTTP service + `cmd/{backup,restore,scanner,cleaner}` CLIs
- Decoupled from `trading/lib`: build-info via public `github.com/bborbe/metrics`, loglevel + sync-producer via public `github.com/bborbe/log` / `github.com/bborbe/kafka`
- Publish-only build → `docker.io/bborbe/kafka-topic-backup:vX.Y.Z`
