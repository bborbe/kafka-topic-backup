include ../../../../Makefile.variables
include ../../../../common.env
include ../../../../local.env

DATADIR ?= /Volumes/kafka-topic-backup
WORKERS ?= 10
TOPICS ?= $(shell kubectlquant -n strimzi get kafkatopics.kafka.strimzi.io --no-headers | awk '{print $$1}' | sort | tr '\n' ',' | sed 's/,$$//')

backup:
	@echo "Backing up all topics with ${WORKERS} workers..."
	@echo "Found $$(echo '${TOPICS}' | tr ',' '\n' | wc -l | tr -d ' ') topics"
	@go run -mod=mod main.go \
		-kafka-brokers="${KAFKA_BROKERS}" \
		-backup-topics="${TOPICS}" \
		-datadir="${DATADIR}" \
		-workers=${WORKERS} \
		-skip-corrupt=true \
		-v=2

# Backup single topic
backup-topic: TOPIC=develop-core-candle-v1-event
backup-topic:
	@echo "=== Backing up ${TOPIC} ==="
	@go run -mod=mod main.go \
		-kafka-brokers="${KAFKA_BROKERS}" \
		-backup-topics="${TOPIC}" \
		-datadir="${DATADIR}" \
		-workers=${WORKERS} \
		-skip-corrupt=true \
		-v=2

# List/count helpers
list-topics:
	@kubectlquant -n strimzi get kafkatopics.kafka.strimzi.io --no-headers | awk '{print $$1}' | sort

count-topics:
	@echo "Topics: $$(kubectlquant -n strimzi get kafkatopics.kafka.strimzi.io --no-headers | wc -l | tr -d ' ')"

# Verify backup status
verify-backups:
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
	echo "Summary: $$complete/$$total complete, $$failed failed"
