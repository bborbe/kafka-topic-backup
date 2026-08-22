include ../Makefile.common

# Check backup status (dry run)
.PHONY: check
check:
	@go run -mod=mod main.go \
	-datadir="${DATADIR}" \
	-dry-run=true \
	-v=2

# Clean failed backups
.PHONY: clean
clean:
	@go run -mod=mod main.go \
	-datadir="${DATADIR}" \
	-v=2
