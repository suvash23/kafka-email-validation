# Email Validation Event Processing System

A production-style Kafka learning project built with Laravel 13, Apache Kafka, and PostgreSQL.

## Quick Start

```bash
# Start all services
docker compose up -d

# Watch logs
docker compose logs -f

# Stop (keep data)
docker compose down

# Stop + delete all data (full reset)
docker compose down -v
```

## Services

| Service | URL | Description |
|---|---|---|
| Laravel API | http://localhost:8000 | REST API |
| Kafka UI | http://localhost:8080 | Web UI for Kafka inspection |
| PostgreSQL | localhost:5432 | Database |
| Kafka | localhost:29092 | Kafka broker (external) |

## Kafka Inspection

```bash
# Make the script executable (once)
chmod +x kafka-scripts/kafka-inspect.sh

# List all topics
./kafka-scripts/kafka-inspect.sh list-topics

# Describe a topic
./kafka-scripts/kafka-inspect.sh describe-topic email-validation

# List consumer groups
./kafka-scripts/kafka-inspect.sh list-groups

# Show consumer lag
./kafka-scripts/kafka-inspect.sh describe-group email-validation-workers

# Read messages from a topic
./kafka-scripts/kafka-inspect.sh consume-topic email-validation

# Send a test message
./kafka-scripts/kafka-inspect.sh produce-test email-validation

# Show broker info
./kafka-scripts/kafka-inspect.sh broker-info
```

## Implementation Phases

- [x] **Phase 1** – Project architecture & Docker environment
- [x] **Phase 2** – Start Kafka & PostgreSQL, verify connectivity
- [x] **Phase 3** – Create and inspect Kafka topics manually
- [x] **Phase 4** – Build a simple Kafka producer
- [x] **Phase 5** – Build a simple Kafka consumer
- [x] **Phase 6** – Understand partitions and offsets
- [x] **Phase 7** – Integrate Laravel API with Kafka producer
- [x] **Phase 8** – Build the email-validation consumer
- [x] **Phase 9** – Persist results in PostgreSQL
- [x] **Phase 10** – Manual offset commits
- [x] **Phase 11** – Idempotency
- [x] **Phase 12** – Retry handling
- [x] **Phase 13** – Dead Letter Queue
- [x] **Phase 14** – Multiple consumers / consumer groups
- [x] **Phase 15** – Consumer failure & rebalancing
- [x] **Phase 16** – Structured logging & metrics
- [x] **Phase 17** – Performance testing
- [x] **Phase 18** – Final cleanup & documentation

## Architecture

```
Laravel API (POST /api/email-validations)
    │
    │ publish event (JSON)
    ▼
Apache Kafka
    │
    ├── email-validation (topic, 3 partitions)
    │       └── Partition by email (deterministic routing)
    │
    └── email-validation-dlq (dead letter queue)

Consumer Workers (email-validation-workers group)
    │
    ├── success ──▶ PostgreSQL
    │
    └── failure ──▶ retry (max 3)
                         │
                         ├── success ──▶ PostgreSQL
                         └── failure ──▶ DLQ
```

## Kafka Concepts Covered

1. **Broker** – The Kafka server process
2. **Topic** – Named message channel
3. **Partition** – Ordered, append-only log within a topic
4. **Message** – Key + Value + Headers + Metadata
5. **Producer** – Publishes messages to topics
6. **Consumer** – Subscribes to topics and reads messages
7. **Consumer Group** – Multiple consumers sharing partition assignment
8. **Offset** – Position of a message within a partition
9. **Offset Commit** – Recording how far a consumer has read
10. **Consumer Rebalancing** – Reassignment of partitions when group membership changes
11. **Partition Key** – Determines which partition a message goes to
12. **At-most-once delivery** – May lose messages, never duplicates
13. **At-least-once delivery** – Never loses messages, may duplicate
14. **Idempotent processing** – Safe to process the same message multiple times
15. **Retry** – Reattempting failed message processing
16. **Dead Letter Queue** – Storage for messages that cannot be processed
17. **Scalability** – Adding consumers to increase throughput
