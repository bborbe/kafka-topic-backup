# Cleaner

Remove failed backup attempts from datadir.

## Quick Start

```bash
# Dry run - show what would be deleted
make check

# Clean failed backups
make clean
```

## CLI Usage

```bash
# Dry run
go run main.go \
  -datadir="/Volumes/Backup/Trading" \
  -dry-run=true \
  -v=2

# Clean
go run main.go \
  -datadir="/Volumes/Backup/Trading" \
  -v=2
```

## Parameters

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `-datadir` | Yes | ./data | Backup storage directory |
| `-dry-run` | No | false | Show what would be deleted |

## What Gets Deleted

Removes partition directories where:
- No `stats.json` exists
- `stats.json` has `success: false`

**Keeps:**
- All successful backups (`success: true`)

## Output

```
my-topic:0 - failed backup (error: CRC corruption)
my-topic:1 - no stats.json
removed failed backup: /data/my-topic/0
removed failed backup: /data/my-topic/1
Summary: 2 failed, 5 successful
```

## Use Cases

- Cleanup after failed backup runs
- Free disk space from incomplete backups
- Prepare for fresh full backup
