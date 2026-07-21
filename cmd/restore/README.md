# Restore

Restore Kafka topics from AVRO backup files.

## Quick Start

```bash
# Restore to same topic name
make restore SOURCE_TOPIC=my-topic TARGET_TOPIC=my-topic

# Restore to different topic
make restore SOURCE_TOPIC=prod-orders TARGET_TOPIC=dev-orders-restored

# List available backups
make list-topics

# Verify backup before restore
make verify-backup TOPIC=my-topic
```

## CLI Usage

```bash
go run main.go \
  -kafka-brokers="kafka-0:9092,kafka-1:9092" \
  -datadir="/Volumes/Backup/Trading" \
  -source-topic="my-topic" \
  -target-topic="my-topic-restored" \
  -v=2
```

## Parameters

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `-kafka-brokers` | Yes | - | Comma-separated broker list |
| `-datadir` | Yes | ./data | Backup storage directory |
| `-source-topic` | Yes | - | Topic name in backup |
| `-target-topic` | Yes | - | Topic to restore to |

## Prerequisites

- Target topic must exist before restore
- Target topic should have sufficient partitions
- No consumer group offset restore (messages only)

## Workflow

1. **List backups**: `make list-topics`
2. **Verify backup**: `make verify-backup TOPIC=my-topic`
3. **Create target topic** (if needed):
   ```bash
   kubectlquant -n strimzi exec -it kafka-0 -- \
     kafka-topics.sh --bootstrap-server localhost:9092 \
     --create --topic my-topic --partitions 3 --replication-factor 2
   ```
4. **Restore**: `make restore SOURCE_TOPIC=my-topic TARGET_TOPIC=my-topic`
5. **Verify**: Run scanner or consume from topic

## Use Cases

- Disaster recovery
- Topic data migration
- Corruption fix (delete + recreate + restore)
