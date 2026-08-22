include ../Makefile.common

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
verify-backup:
	@echo "Verifying backup for ${TOPIC}:"
	@for p in "${DATADIR}/${TOPIC}"/*/stats.json; do \
		if [ -f "$$p" ]; then \
			echo "$$p:"; \
			cat "$$p" | jq .; \
		fi; \
	done
