# Backup

Backup Kafka topics to AVRO files.

## Quick Start

```bash
# Backup all topics
make backup

# Backup single topic
make backup-topic TOPIC=my-topic

# Custom workers/datadir
make backup WORKERS=20 DATADIR=/mnt/backup

# Verify backups
make verify-backups
```

## CLI Usage

```bash
go run main.go \
  -kafka-brokers="kafka-0:9092,kafka-1:9092" \
  -backup-topics="topic1,topic2" \
  -datadir="/Volumes/Backup/Trading" \
  -workers=10 \
  -skip-corrupt=true \
  -v=2
```

## Parameters

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `-kafka-brokers` | Yes | - | Comma-separated broker list |
| `-backup-topics` | Yes | - | Topics to backup |
| `-datadir` | Yes | ./data | Backup storage directory |
| `-workers` | No | 10 | Parallel workers |
| `-skip-corrupt` | No | false | Skip corrupt messages |

## Storage Layout

```
/data/
  topic-name/
    0/
      topic.avro      # Message data
      stats.json      # Backup metadata
    1/
      topic.avro
      stats.json
```

## Helpers

```bash
# List topics
make list-topics

# Count topics
make count-topics

# Verify backup status
make verify-backups
```

## Incremental Backup

Automatically resumes from last successful offset if `stats.json` exists with `success: true`.

## Use Cases

- Daily automated backups (CronJob)
- Pre-maintenance snapshots
- Disaster recovery preparation
