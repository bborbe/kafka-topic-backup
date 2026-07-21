include ../../../../Makefile.variables
include ../../../../common.env
include ../../../../local.env

scan: TOPIC=develop-core-candle-v1-event
scan: PARTITION=0
scan: START_OFFSET=0
scan:
	@echo "=== Scanning ${TOPIC} from offset ${START_OFFSET} ==="
	@go run -mod=mod main.go \
	-kafka-brokers="${KAFKA_BROKERS}" \
	-scan-topics="${TOPIC}" \
	-partition=${PARTITION} \
	-start-offset=${START_OFFSET} \
	-workers=1 \
	-v=2
