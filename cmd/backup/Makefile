include ../Makefile.common

WORKERS ?= 10
TOPICS ?= $(shell $(KUBECTL_KAFKA) -n strimzi get kafkatopics.kafka.strimzi.io --no-headers 2>/dev/null | awk '{print $$1}' | sort | tr '\n' ',' | sed 's/,$$//')

# TOPICS comes from a $(shell ...) that cannot signal failure -- if kubectlquant is
# missing (it is a zsh function, absent from make's non-interactive shell), or the
# cluster is unreachable, it expands to the empty string. Backing up "no topics"
# then looks like a clean run. Refuse instead.
.PHONY: require-topics
require-topics:
	@if [ -z '$(TOPICS)' ]; then \
		echo 'ERROR: no topics resolved from $(KUBECTL_KAFKA).' >&2; \
		echo '  kubectlquant is a zsh function -- run this from an interactive zsh,' >&2; \
		echo '  or pass the list explicitly: make backup TOPICS=a,b,c' >&2; \
		exit 1; \
	fi

.PHONY: require-jq
require-jq:
	@command -v jq >/dev/null 2>&1 || { echo 'ERROR: jq is required by this target -- brew install jq' >&2; exit 1; }

.PHONY: backup
backup: require-topics
	@echo "Backing up all topics with ${WORKERS} workers..."
	@echo "Found $$(echo '${TOPICS}' | tr ',' '\n' | wc -l | tr -d ' ') topics"
	@go run -mod=mod main.go \
		-kafka-brokers="${KAFKA_BROKERS}" \
		-backup-topics="${TOPICS}" \
		-datadir="${DATADIR}" \
		-workers="${WORKERS}" \
		-skip-corrupt=true \
		-v=2

# Backup single topic
.PHONY: backup-topic
backup-topic: TOPIC=develop-core-candle-v1-event
backup-topic:
	@echo "=== Backing up ${TOPIC} ==="
	@go run -mod=mod main.go \
		-kafka-brokers="${KAFKA_BROKERS}" \
		-backup-topics="${TOPIC}" \
		-datadir="${DATADIR}" \
		-workers="${WORKERS}" \
		-skip-corrupt=true \
		-v=2

# List/count helpers
.PHONY: list-topics
list-topics:
	@$(KUBECTL_KAFKA) -n strimzi get kafkatopics.kafka.strimzi.io --no-headers | awk '{print $$1}' | sort

.PHONY: count-topics
count-topics:
	@echo "Topics: $$($(KUBECTL_KAFKA) -n strimzi get kafkatopics.kafka.strimzi.io --no-headers | wc -l | tr -d ' ')"

# Verify backup status
.PHONY: verify-backups
verify-backups: require-jq
	@echo "Verifying backups in ${DATADIR}..."
	@total=0; complete=0; failed=0; \
	for topic_dir in ${DATADIR}/*/; do \
		if [ -d "$$topic_dir" ]; then \
			topic=$$(basename "$$topic_dir"); \
			for part_dir in $$topic_dir*/; do \
				if [ -d "$$part_dir" ]; then \
					total=$$((total+1)); \
					part=$$(basename "$$part_dir"); \
					if [ -f "$$part_dir/stats.json" ]; then \
						success=$$(jq -r '.success // false' "$$part_dir/stats.json" 2>/dev/null); \
						if [ "$$success" = "true" ]; then \
							complete=$$((complete+1)); \
						else \
							failed=$$((failed+1)); \
							printf "✗ %s:%s\n" "$$topic" "$$part"; \
						fi; \
					else \
						failed=$$((failed+1)); \
						printf "⚠ %s:%s (no stats)\n" "$$topic" "$$part"; \
					fi; \
				fi; \
			done; \
		fi; \
	done; \
	if [ "$$total" -eq 0 ]; then \
		echo "ERROR: no backup partitions found under ${DATADIR} -- nothing was verified." >&2; \
		echo "  Is the backup volume mounted?" >&2; \
		exit 1; \
	fi; \
	echo "Summary: $$complete/$$total complete, $$failed failed"
