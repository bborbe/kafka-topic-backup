# kafka-topic-backup

Kafka topic backup and restore toolkit. Backs up topic messages to the local filesystem in Avro format with incremental, corruption-tolerant, per-partition backups. Broker-agnostic.

## Architecture

- **Four commands**: `backup`, `restore`, `scanner`, `cleaner`
- **AVRO format**: Uses schema `BackupTopicRecord` for messages
- **Per-partition backup**: Each partition backed up independently
- **Incremental**: Resumes from last offset if backup exists
- **Corruption-tolerant**: Optional `--skip-corrupt` to skip CRC errors

## Commands

### Backup Command

Backs up Kafka topics to local filesystem with per-partition granularity.

#### Usage

```bash
cd cmd/backup

# Backup all topics (auto-discovers via kubectl)
make backup

# Backup specific topics
make backup TOPICS=topic-a,topic-b

# Backup single topic
make backup-topic TOPIC=my-topic

# Custom workers/datadir
make backup WORKERS=20 DATADIR=/mnt/backup

# List available topics
make list-topics

# Verify backup status
make verify-backups
```

#### Makefile Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TOPICS` | Auto-discovered | Comma-separated topics (from Strimzi CRD) |
| `DATADIR` | `/Volumes/Backup` | Backup directory |
| `WORKERS` | `10` | Parallel backup workers |

#### Direct execution

```bash
go run main.go \
  -kafka-brokers="kafka-0:9092,kafka-1:9092" \
  -backup-topics="topic-a,topic-b" \
  -datadir="./data" \
  -workers=10 \
  -skip-corrupt=true \
  -v=2
```

#### Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `-kafka-brokers` | Yes | - | Comma separated Kafka brokers |
| `-backup-topics` | Yes | - | Comma separated topics to backup |
| `-datadir` | Yes | `./data` | Data directory for backups |
| `-workers` | No | `10` | Number of parallel backup workers |
| `-skip-corrupt` | No | `false` | Skip corrupt records instead of failing |
| `-sentry-dsn` | No | - | Sentry DSN for error reporting |
| `-v` | No | `0` | Log verbosity (0-4) |

#### Incremental Backup

Backups are incremental by default:
- If `stats.json` exists with `success: true`, resumes from `last_offset + 1`
- New messages are appended to existing `data.avro`
- To start fresh, use a different `--datadir` path

```bash
# Resume existing backup (same datadir)
make backup DATADIR=/backup/current

# Start fresh backup (new datadir)
make backup DATADIR=/backup/2025-01-15
```

#### Skip Corrupt Records

The Makefile enables `--skip-corrupt` by default. When corruption is detected:
1. Uses exponential + binary search to find next healthy offset
2. Skips corrupt range and continues backup
3. Records skipped ranges in `stats.json`

### Restore Command

Restores topic messages from backup to a target topic.

#### Usage

```bash
cd cmd/restore

# Restore with default topic mapping
make run

# Restore specific topic
make restore SOURCE_TOPIC=my-topic TARGET_TOPIC=my-topic-restored
```

#### Direct execution

```bash
go run main.go \
  -kafka-brokers="kafka-0:9092" \
  -source-topic="my-topic" \
  -target-topic="my-topic-restored" \
  -datadir="./data" \
  -v=2
```

### Scanner Command

Scans Kafka topics for CRC corruption errors and reports exact corrupt offset ranges.

#### Usage

```bash
cd cmd/scanner

# Scan specific topic
make scan TOPIC=my-topic

# Scan specific partition
make scan TOPIC=my-topic PARTITION=1

# Resume from known corrupt offset (skip good messages)
make scan TOPIC=my-topic START_OFFSET=7910000

# Scan all partitions
make scan TOPIC=my-topic PARTITION=-1
```

#### Direct execution

```bash
go run main.go \
  -kafka-brokers="kafka-0:9092" \
  -scan-topics="topic-a,topic-b" \
  -workers=5 \
  -partition=-1 \
  -v=2
```

#### Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `-scan-topics` | Yes | - | Comma separated topics to scan |
| `-partition` | No | `-1` | Specific partition (-1 = all) |
| `-start-offset` | No | `-1` | Start offset (-1 = oldest) |
| `-workers` | No | `5` | Parallel scan workers |

#### Features

- **Adaptive skip**: Exponential search + binary search to quickly find corruption boundaries
- **Compaction-aware**: Distinguishes between actual corruption and compaction gaps
- **Progress tracking**: Reports progress every 10 seconds during long scans
- **Parallel scanning**: Multiple workers for scanning multiple topics

#### Output

```
develop-core-candle-v1-event
  Total messages: 81,130,701
  ✗ Corrupt ranges: 10
    Partition 0: offsets 7,910,000 - 7,923,505 (13,506 messages)
      Error: CRC didn't match expected 0x1c62625e got 0x9f536534
    ...
  Scan duration: 425.9s
```

#### Performance

- **Small ranges** (10-50k offsets): ~3-5 seconds
- **Large topics** (700M offsets): ~7-10 minutes per partition
- **Compaction gaps**: Automatically detected and skipped

### Cleaner Command

Verifies backup status and cleans up failed backups.

#### Usage

```bash
cd cmd/cleaner

# Check backup status (dry run)
make check

# Clean failed backups
make clean
```

## Storage Structure

Backups are organized by topic and partition:

```
./data/
└── my-topic/
    ├── 0/
    │   ├── data.avro     # Partition 0 messages
    │   └── stats.json    # Partition 0 metadata
    ├── 1/
    │   ├── data.avro     # Partition 1 messages
    │   └── stats.json    # Partition 1 metadata
    └── 2/
        ├── data.avro
        └── stats.json
```

**stats.json format:**
```json
{
  "topic": "my-topic",
  "partition": 0,
  "started_at": "2025-01-15T10:00:00Z",
  "completed_at": "2025-01-15T10:05:00Z",
  "duration_seconds": 300.5,
  "message_count": 125000,
  "first_offset": 0,
  "last_offset": 124999,
  "skipped_ranges": [
    {"start": 50000, "end": 50099}
  ],
  "success": true
}
```

- `success: true` indicates completed backup
- `first_offset`/`last_offset` track backup range
- `skipped_ranges` records any corruption skipped (when `--skip-corrupt` used)

## How It Works

1. **Discovery**: Queries Kafka for topic partitions
2. **Per-partition backup**: Each partition processed independently
3. **Resume check**: Reads existing `stats.json` for last offset
4. **Consume**: Reads from `last_offset + 1` to highwater mark
5. **Write**: Appends messages to AVRO file
6. **Stats**: Updates `stats.json` with new offsets

### Corruption Handling (--skip-corrupt)

When CRC errors are detected:
1. Closes current consumer
2. Uses exponential search (+10, +100, +1000, ...) to find healthy offset
3. Binary search refines exact corruption boundary
4. Records skipped range and continues

## Monitoring

### Prometheus Metrics

- `strimzi_backup_topic_started` - Backup started counter
- `strimzi_backup_topic_completed` - Backup completed counter
- `strimzi_backup_topic_failed` - Backup failed counter
- `strimzi_backup_topic_message_count` - Messages backed up
- `strimzi_backup_topic_duration_seconds` - Backup duration

## Build

`make buca` builds and publishes `docker.io/bborbe/kafka-topic-backup:vX.Y.Z` (git-tag semver).
