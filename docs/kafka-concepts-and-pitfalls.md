# Kafka Concepts & Pitfalls

*This document serves as a growing repository of the Kafka concepts and "what can go wrong" scenarios learned during each phase of the Email Validation Event Processing System learning project.*

---

## Phase 1 — Project architecture & Docker environment

### Kafka Concepts You Will Learn
- **Broker**: The Kafka server process. Receives messages from producers, stores them on disk in partitions, serves them to consumers. In production you run 3+ brokers for fault tolerance.
- **KRaft mode**: Kafka without Zookeeper. Kafka manages its own metadata (topic list, partition leaders) using the Raft consensus algorithm. Simpler stack, required since Kafka 3.x.
- **Listener**: A named network address Kafka binds to. We defined three: `INTERNAL` (container-to-container), `EXTERNAL` (host machine), `CONTROLLER` (KRaft leader election).

### What can go wrong
- **Port Conflicts**: Port 9092 could already be in use on the host machine.
- **UUID requirement**: `CLUSTER_ID` for KRaft mode must be a valid base64-encoded UUID.

---

## Phase 2 — Start Kafka & PostgreSQL, verify connectivity

### Kafka Concepts You Will Learn
- **Startup Sequence**: Kafka must elect a KRaft controller leader before it can serve clients. The broker registers itself, loads log segments, and only then starts accepting connections.
- **Healthchecks**: Without making standard services wait for a positive health response via something like `kafka-broker-api-versions`, dependent API apps might crash instantly on boot.

### What can go wrong
- **False health**: Dependent services might boot before Kafka completes leader election, causing initialization crashes.

---

## Phase 3 — Create and Inspect Kafka Topics Manually

### Kafka Concepts You Will Learn
- **Topic**: A named, ordered, append-only log. Messages written to a topic are never removed when a consumer reads them. They stay until the retention period expires.
- **Partition**: A topic is split into N partitions (independent ordered logs). Partitions enable parallelism. The partition count is fixed at creation and determines maximum consumer parallelism.
- **Replication Factor**: How many broker copies of each partition exist.
- **Leader/ISR**: For each partition, one broker is the Leader (handles all reads and writes). ISR (In-Sync Replicas) are followers that are fully caught up.

### What can go wrong
- **Unrealistic Replication**: Trying to set `--replication-factor 3` on a single-broker local cluster will fail.
- **Partition Deletion**: You can increase partition count, but you can NEVER decrease partitions without wiping the topic completely.

---

## Phase 4 — Build a simple Kafka producer

### Kafka Concepts You Will Learn
- **Producer**: A client that connects to the broker, serializes data, and writes it to a topic.
- **RD_KAFKA_PARTITION_UA**: Tells Kafka to pick the partition itself (using round-robin if no key is provided) to balance load.
- **flush(timeout_ms)**: Messages are queued locally in memory by the producer. `flush()` forces the network send and blocks until the broker acknowledges.
- **Ack (acknowledgement)**: Controls write safety. `acks=0` (fire and forget), `acks=1` (leader confirms), `acks=all` (all ISR confirm).

### What can go wrong
- **Script exits before flush**: If your PHP script finishes without calling `flush()`, queued messages in memory are lost entirely.
- **Timeout failure**: If the broker is unreachable, `flush()` will block until it times out.

---

## Phase 5 — Build a simple Kafka consumer

### Kafka Concepts You Will Learn
- **Consumer**: A client that pulls messages from a topic. Reading does NOT remove the message; the consumer tracks its read position via an *offset*.
- **group.id**: Identifies the consumer group. Kafka uses this to track offsets per group. Different groups read independently.
- **auto.offset.reset**: `earliest` starts from the very first message; `latest` starts from new messages only.
- **consume(timeout)**: Reading is a blocking action inside an infinite loop.
- **Auto-commit**: On by default. Offsets are saved in the background every 5 seconds regardless of successful processing (*at-most-once delivery*).

### What can go wrong
- **Treating _TIMED_OUT as an error**: When no new messages arrive in the given time block, the library returns a timeout status. This is normal (idle state), not a failure!
- **Treating _PARTITION_EOF as an error**: This simply means you hit the current end of the log.
- **Forgetting the loop**: Consumers are daemons. If you don't use `while(true)`, it reads one message and quits.

---

## Phase 6 — Understand partitions and offsets (Coming Up!)

*(Content will be added upon completion of Phase 6)*
