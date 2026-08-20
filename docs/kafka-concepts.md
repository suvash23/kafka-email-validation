# Kafka Concepts Dictionary

| Concept | Project Example | Definition |
|---------|-----------------|------------|
| **Broker** | `kafka:9092` | The Kafka server storing and distributing messages. |
| **Topic** | `email-validation` | A designated append-only log channel for related events. |
| **Partition** | 3 partitions for `email-validation` | Splits topics for concurrency. One consumer thread reads from one partition (max 3 concurrent consumers in our project). |
| **Producer** | `KafkaProducerService.php` | The client sending messages into the topic. |
| **Consumer** | `ConsumeEmailValidations.php` | The client subscribing to topics to fetch, process, and commit offset on messages. |
| **Consumer Group** | `email-validation-workers` | Groups consumers together so they can share the partition load and track offsets logically. |
| **Offset** | N/A | The sequential ID for a message inside a single partition. |
| **Rebalancing** | Taking 1 worker down | When consumers drop out or join, Kafka redistributes the partitions among active members. |
| **DLQ (Dead Letter Queue)** | `email-validation-dlq` | Where permanently failed/errored messages end up, keeping the main loop unblocked. |
| **Partition Key** | User's Email Address | Ensures all validation requests tied to `user@example.com` go to the same specific partition and are ordered. |
| **Idempotency** | Event ID deduplication | Processing the exact same Kafka event multiple times doesn't break consistency or add duplicates to DB. |
