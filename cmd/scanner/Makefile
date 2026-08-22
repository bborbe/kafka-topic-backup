include ../Makefile.common

TOPIC        ?= develop-core-candle-v1-event
PARTITION    ?= 0
START_OFFSET ?= 0

.PHONY: scan
scan:
	@echo "=== Scanning ${TOPIC} from offset ${START_OFFSET} ==="
	@go run -mod=mod main.go \
	-kafka-brokers="${KAFKA_BROKERS}" \
	-scan-topics="${TOPIC}" \
	-partition=${PARTITION} \
	-start-offset=${START_OFFSET} \
	-workers=1 \
	-v=2
