include ../Makefile.common

# Check backup status (dry run)
check:
	@go run -mod=mod main.go \
	-datadir="${DATADIR}" \
	-dry-run=true \
	-v=2

# Clean failed backups
clean:
	@go run -mod=mod main.go \
	-datadir="${DATADIR}" \
	-v=2
