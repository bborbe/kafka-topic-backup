# Scanner

Scan Kafka topics for CRC corruption.

## Quick Start

```bash
# Scan single topic/partition
make scan TOPIC=my-topic PARTITION=0

# Scan from specific offset
make scan TOPIC=my-topic PARTITION=0 START_OFFSET=1000
```

## CLI Usage

```bash
go run main.go \
  -kafka-brokers="kafka-0:9092,kafka-1:9092" \
  -scan-topics="topic1,topic2" \
  -partition=0 \
  -start-offset=0 \
  -workers=1 \
  -v=2
```

## Parameters

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `-kafka-brokers` | Yes | - | Comma-separated broker list |
| `-scan-topics` | Yes | - | Topics to scan |
| `-partition` | No | -1 | Specific partition (-1 = all) |
| `-start-offset` | No | -1 | Start offset (-1 = oldest) |
| `-workers` | No | 5 | Parallel workers |

## Output

```
[Worker 0] my-topic
  Total messages: 1,876
  ✓ No corruption detected
  Scan duration: 0.5s
```

Or if corrupted:

```
[Worker 0] my-topic
  Total messages: 707,363,983
  ✗ Corrupt ranges: 1
    Partition 0: offsets 7,532,642 - 7,532,644 (3 messages)
      Error: CRC didn't match expected 0x1c62625e got 0x9f536534
  Scan duration: 45.2s
```

## Use Cases

- Verify topic integrity after restore
- Find exact corrupt offset ranges
- Pre-backup validation
