include ../Makefile.common

.PHONY: require-jq
require-jq:
	@command -v jq >/dev/null 2>&1 || { echo 'ERROR: jq is required by this target -- brew install jq' >&2; exit 1; }

run: SOURCE_TOPIC=develop-core-symbol-v1-event
run: TARGET_TOPIC=develop-core-symbol-v1-event-restored
run: restore

restore:
	@go run -mod=mod main.go \
	-kafka-brokers="${KAFKA_BROKERS}" \
	-source-topic="${SOURCE_TOPIC}" \
	-target-topic="${TARGET_TOPIC}" \
	-datadir="${DATADIR}" \
	-v=2

list-topics:
	@echo "Available topics in ${DATADIR}:"
	@ls -1 "${DATADIR}" 2>/dev/null || echo "No backups found"

verify-backup: TOPIC=develop-core-symbol-v1-event
verify-backup: require-jq
	@echo "Verifying backup for ${TOPIC}:"
	@for p in "${DATADIR}/${TOPIC}"/*/stats.json; do \
		if [ -f "$$p" ]; then \
			echo "$$p:"; \
			cat "$$p" | jq .; \
		fi; \
	done
