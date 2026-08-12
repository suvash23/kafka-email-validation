# Email Validation Event Processing System — Learning Plan

> **Stack:** Laravel 13 · PHP 8.3+ · Apache Kafka (KRaft) · PostgreSQL 16 · Docker Compose
> **Primary goal:** Learn Kafka concepts deeply through incremental, phase-by-phase implementation.
> **Rule:** Do not move to the next phase until explicitly ready.

---

## How Each Phase Is Structured

Every phase follows this format:

1. **Kafka concepts you will learn** — what, why, internals
2. **What can go wrong** — real failure modes
3. **Implementation** — files created/modified
4. **Commands to run**
5. **How to test it**
6. **Expected output**
7. **Common mistakes**

---

## Progress Tracker

- [x] **Phase 1** — Project architecture & Docker environment
- [x] **Phase 2** — Start Kafka & PostgreSQL, verify connectivity
- [x] **Phase 3** — Create and inspect Kafka topics manually
- [x] **Phase 4** — Build a simple Kafka producer (standalone PHP)
- [x] **Phase 5** — Build a simple Kafka consumer (standalone PHP)
- [x] **Phase 6** — Understand partitions and offsets
- [x] **Phase 7** — Integrate Laravel API with Kafka producer
- [x] **Phase 8** — Build the email-validation consumer command
- [x] **Phase 9** — Persist results in PostgreSQL
- [x] **Phase 10** — Manual offset commits
- [x] **Phase 11** — Idempotency
- [x] **Phase 12** — Retry handling
- [ ] **Phase 13** — Dead Letter Queue ← **YOU ARE HERE**
- [ ] **Phase 14** — Multiple consumers & consumer groups
- [ ] **Phase 15** — Consumer failure & rebalancing
- [ ] **Phase 16** — Structured logging & metrics
- [ ] **Phase 17** — Performance testing
- [ ] **Phase 18** — Final cleanup & documentation

---

## Phase 1 — Project Architecture & Docker Environment ✅

### Kafka concepts you learned

| Concept | What it is |
|---|---|
| **Broker** | The Kafka server process. Receives messages from producers, stores them on disk in partitions, serves them to consumers. In production you run 3+ brokers for fault tolerance. |
| **KRaft mode** | Kafka without Zookeeper. Kafka manages its own metadata (topic list, partition leaders) using the Raft consensus algorithm. Simpler stack, required since Kafka 3.x. |
| **Listener** | A named network address Kafka binds to. We define three: `INTERNAL` (container-to-container), `EXTERNAL` (host machine), `CONTROLLER` (KRaft leader election). |

### What was built

| File | Purpose |
|---|---|
| `docker-compose.yml` | Four services: `kafka`, `kafka-ui`, `postgres`, `app` |
| `docker/app/Dockerfile` | PHP 8.3-cli-alpine with `pdo_pgsql`, `mbstring`, `pcntl`, `rdkafka` |
| `docker/postgres/init.sql` | Enables `uuid-ossp` and `pgcrypto` PostgreSQL extensions |
| `kafka-scripts/kafka-inspect.sh` | Host-side wrapper for Kafka CLI commands |
| `laravel/` | Laravel 13.x application (installed via `composer create-project`) |
| `.env` | Environment variables for DB and Kafka config |

### Architecture

```
POST /api/email-validations
        │
        │  Laravel API (port 8000)
        │  publishes JSON event
        ▼
Apache Kafka (port 9092 / 29092)
        │
        ├── email-validation topic (3 partitions)
        │       ├── Partition 0
        │       ├── Partition 1
        │       └── Partition 2
        │
        └── email-validation-dlq topic (1 partition)

Consumer Workers (group: email-validation-workers)
        │
        ├── success ──▶ PostgreSQL (port 5432)
        └── failure ──▶ retry (max 3) ──▶ DLQ

Kafka UI (port 8080) — web inspection tool
```

---

## Phase 2 — Start Kafka & PostgreSQL, Verify Connectivity

### Kafka concepts you will learn

**What Kafka is doing at startup:**
Kafka must elect a KRaft controller leader before it can serve clients. This is why it needs a `start_period` in the healthcheck — it takes 10–20 seconds for the cluster to initialize. The broker registers itself with the controller quorum, loads its log segments from disk, and only then starts accepting connections.

**Why healthchecks matter:**
Docker's `depends_on: condition: service_healthy` means Laravel and Kafka UI won't start until Kafka responds to `kafka-broker-api-versions`. Without this, the app would crash on startup because the broker isn't ready.

**What can go wrong:**
- Kafka fails to start if `CLUSTER_ID` is malformed (must be base64 UUID)
- Port conflicts if `9092` or `29092` are already bound on your host
- PostgreSQL `init.sql` only runs on first creation; if the volume exists, it's skipped

### Files created/modified
None — everything was created in Phase 1.

### Commands to run

```bash
# Start all services (detached)
docker compose up -d

# Watch startup logs
docker compose logs -f

# Check all containers are healthy
docker compose ps
```

### How to test it

```bash
# 1. Verify Kafka broker is accepting connections
./kafka-scripts/kafka-inspect.sh broker-info

# 2. Verify PostgreSQL is up and database exists
docker exec -it postgres psql -U validator -d email_validation -c '\l'

# 3. Verify PostgreSQL extensions were created
docker exec -it postgres psql -U validator -d email_validation -c '\dx'

# 4. Verify Laravel responds
curl http://localhost:8000

# 5. Verify Kafka UI is accessible
open http://localhost:8080
```

### Expected output

```
# docker compose ps
NAME           STATUS          PORTS
kafka          Up (healthy)    0.0.0.0:9092->9092, 0.0.0.0:29092->29092
kafka-ui       Up              0.0.0.0:8080->8080
postgres       Up (healthy)    0.0.0.0:5432->5432
laravel-app    Up              0.0.0.0:8000->8000

# broker-info
Latest supported version: 3 to 7
LeaderId: 1, LeaderEpoch: 1, HighWatermark: ...

# psql \dx
 uuid-ossp | ... | Functions for generating ...
 pgcrypto   | ... | cryptographic functions
```

### Common mistakes

| Mistake | Fix |
|---|---|
| Kafka stuck "starting" | Run `docker compose logs kafka` — usually a port conflict or bad `CLUSTER_ID` |
| `laravel-app` keeps restarting | It's waiting for Kafka/Postgres healthchecks; give it 60 seconds |
| `init.sql` extensions missing | `docker compose down -v` then `docker compose up -d` to recreate the volume |

---

## Phase 3 — Create and Inspect Kafka Topics Manually

### Kafka concepts you will learn

**Topic:** A named, ordered, append-only log. Messages written to a topic are never removed when a consumer reads them. They stay until the retention period expires (we set 7 days). This is fundamentally different from a traditional queue.

**Partition:** A topic is split into N partitions. Each partition is an independent ordered log. Partitions enable parallelism — multiple consumers can read different partitions simultaneously. The partition count is fixed at creation and determines your maximum consumer parallelism.

**Replication Factor:** How many broker copies of each partition exist. With `replication-factor=1` (our dev setup), if the broker dies, data is lost. In production you'd use `replication-factor=3`.

**Leader/ISR:** For each partition, one broker is the Leader (handles all reads and writes). ISR = In-Sync Replicas — followers fully caught up to the leader. If the leader dies, Kafka promotes an ISR member.

**What can go wrong:**
- Creating a topic with `replication-factor > number of brokers` fails — we only have 1 broker
- Deleting and recreating a topic with the same name while consumers are running causes partition reassignment confusion

### Commands to run

```bash
# Create the main topic: 3 partitions, replication-factor 1
docker exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic email-validation \
  --partitions 3 \
  --replication-factor 1

# Create the Dead Letter Queue topic: 1 partition
docker exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic email-validation-dlq \
  --partitions 1 \
  --replication-factor 1
```

### How to test it

```bash
# List all topics
./kafka-scripts/kafka-inspect.sh list-topics

# Describe the main topic — shows partitions, leader, ISR
./kafka-scripts/kafka-inspect.sh describe-topic email-validation

# Describe the DLQ topic
./kafka-scripts/kafka-inspect.sh describe-topic email-validation-dlq

# Also verify in Kafka UI
open http://localhost:8080  # → Topics
```

### Expected output

```
# list-topics
email-validation
email-validation-dlq

# describe-topic email-validation
Topic: email-validation     PartitionCount: 3   ReplicationFactor: 1
  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

### Kafka concepts learned
- A topic with 3 partitions can be consumed by at most 3 consumers concurrently
- `Leader: 1` = broker with node ID 1 is the leader for all partitions (makes sense — we only have one broker)
- `Isr: 1` = the leader is its own only in-sync replica

### Common mistakes

| Mistake | Fix |
|---|---|
| `replication-factor 3` fails | You only have 1 broker; use `--replication-factor 1` |
| Topic already exists error | Use `--if-not-exists` flag or delete first with `--delete` |
| Partitions can't be decreased | You can only increase partition count, never decrease |

---

## Phase 4 — Build a Simple Kafka Producer (Standalone PHP)

### Kafka concepts you will learn

**Producer:** A client that writes (produces) messages to a Kafka topic. It connects to the broker, serializes data, and sends it. The producer doesn't care about consumers — it just fires messages into the topic.

**`RD_KAFKA_PARTITION_UA` (Unassigned):** When you don't specify a partition, Kafka uses round-robin across partitions. Once we add a message key (Phase 6), the key's hash determines the partition.

**`flush(timeout_ms)`:** Messages are batched in memory before being sent. `flush()` forces all pending messages to be sent and waits for broker acknowledgement. Without it, your script could exit before messages are delivered.

**Ack (acknowledgement):** When the broker writes the message to its log, it sends an ack back to the producer. The `acks` setting controls this:
- `acks=0` — fire and forget (at-most-once; fastest, can lose data)
- `acks=1` — leader confirms write (default)
- `acks=all` — all ISR replicas confirm (safest, slowest)

**What can go wrong:**
- Script exits before `flush()` completes → messages silently dropped
- Broker unreachable → `flush()` times out; check error callback
- Using `acks=0` in learning → you'll never know if messages were actually stored

### Files created

`laravel/scripts/produce-test.php`

```php
<?php
declare(strict_types=1);

/**
 * Standalone Kafka Producer — Phase 4
 *
 * KAFKA CONCEPT: Producer
 * This script connects directly to the Kafka broker and publishes
 * a single message to the email-validation topic.
 *
 * Run inside the container:
 *   docker exec laravel-app php scripts/produce-test.php
 */

$conf = new RdKafka\Conf();

// The broker address (inside Docker network)
$conf->set('metadata.broker.list', env_or('KAFKA_BROKERS', 'kafka:9092'));

// CONCEPT: acks=1 means the leader broker must confirm the write.
// acks=0 would be fire-and-forget (may lose messages).
$conf->set('acks', '1');

// Error callback — called if delivery fails
$conf->setDrMsgCb(function (RdKafka\Producer $kafka, RdKafka\Message $message) {
    if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        echo "[ERROR] Delivery failed: " . $message->errstr() . PHP_EOL;
    } else {
        echo "[OK] Delivered to partition {$message->partition} at offset {$message->offset}" . PHP_EOL;
    }
});

$producer = new RdKafka\Producer($conf);

// Get a handle to the topic
$topic = $producer->newTopic('email-validation');

// Build the message payload
$payload = json_encode([
    'event_id'     => sprintf('%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
        mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff),
        mt_rand(0, 0x0fff) | 0x4000, mt_rand(0, 0x3fff) | 0x8000,
        mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff)),
    'event_type'   => 'email.validation.requested',
    'email'        => 'test@example.com',
    'requested_at' => date('c'),
]);

// CONCEPT: RD_KAFKA_PARTITION_UA = "unassigned" — Kafka picks the partition (round-robin)
// In Phase 6 we'll specify a key to control partition routing.
$topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload);

echo "Sending message..." . PHP_EOL;

// CONCEPT: flush() — block until all queued messages are delivered (or timeout)
// Without this, the script exits and messages in the internal queue are lost.
$result = $producer->flush(10000); // 10 second timeout
if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    echo "[ERROR] Not all messages were flushed. Error code: {$result}" . PHP_EOL;
    exit(1);
}

function env_or(string $key, string $default): string {
    return getenv($key) ?: $default;
}
```

### Commands to run

```bash
# Run the producer inside the Laravel container
docker exec laravel-app php scripts/produce-test.php

# Run it 3 times to produce 3 messages
for i in 1 2 3; do docker exec laravel-app php scripts/produce-test.php; done
```

### How to test it

```bash
# Verify the message exists in Kafka
./kafka-scripts/kafka-inspect.sh consume-topic email-validation
# Should print the JSON message(s)

# Check in Kafka UI: Topics → email-validation → Messages
open http://localhost:8080
```

### Expected output

```
Sending message...
[OK] Delivered to partition 1 at offset 0
```

### Common mistakes

| Mistake | Fix |
|---|---|
| Script exits silently, no messages | Missing `flush()` call |
| "Broker transport failure" | Wrong broker address; inside Docker use `kafka:9092`, not `localhost:9092` |
| `flush()` returns non-zero | Broker unreachable or network issue; check `docker compose ps` |

---

## Phase 5 — Build a Simple Kafka Consumer (Standalone PHP)

### Kafka concepts you will learn

**Consumer:** A client that reads (consumes) messages from a topic. Unlike a traditional queue, reading a message does NOT remove it. The message stays in the partition; the consumer tracks its own read position using an **offset**.

**`group.id`:** Identifies which consumer group this process belongs to. Kafka uses this to track offsets per group. Two groups reading the same topic have completely independent offsets — they don't interfere with each other.

**`auto.offset.reset`:**
- `earliest` — start from the very first message in the partition (good for learning; replays everything)
- `latest` — start from the next new message (production default; skips history)

**`consume(timeout_ms)`:** Blocks up to N milliseconds waiting for a message. If no message arrives, it returns a message with `err = RD_KAFKA_RESP_ERR__TIMED_OUT`. This is normal — not an error.

**Auto-commit:** By default, rdkafka commits the offset automatically every 5 seconds in the background. This is convenient but dangerous: the offset is committed before you confirm successful processing, which is "at-most-once" delivery. We'll fix this in Phase 10.

**What can go wrong:**
- Treating `_TIMED_OUT` as a real error and crashing the consumer loop
- Not handling `_PARTITION_EOF` (reached the end of a partition — normal, not an error)
- Forgetting that `consume()` is blocking; running it in a web request would block the HTTP response

### Files created

`laravel/scripts/consume-test.php`

```php
<?php
declare(strict_types=1);

/**
 * Standalone Kafka Consumer — Phase 5
 *
 * KAFKA CONCEPT: Consumer
 * This script joins the email-validation-workers consumer group
 * and reads messages from the email-validation topic indefinitely.
 *
 * Run inside the container:
 *   docker exec -it laravel-app php scripts/consume-test.php
 *
 * Stop with Ctrl+C.
 */

$conf = new RdKafka\Conf();

// CONCEPT: group.id — identifies this consumer's group.
// Kafka tracks committed offsets per (group + topic + partition).
$conf->set('group.id', env_or('KAFKA_CONSUMER_GROUP', 'email-validation-workers'));

$conf->set('metadata.broker.list', env_or('KAFKA_BROKERS', 'kafka:9092'));

// CONCEPT: auto.offset.reset
// 'earliest' → replay all messages from the beginning of the partition.
// 'latest'   → only read new messages produced after this consumer started.
$conf->set('auto.offset.reset', 'earliest');

// NOTE: auto-commit is ON by default (commits every 5s in background).
// This is "at-most-once" delivery — you might miss messages on crash.
// We will disable this in Phase 10 and commit manually.
$conf->set('enable.auto.commit', 'true');

$consumer = new RdKafka\KafkaConsumer($conf);

// Subscribe to a topic (Kafka handles partition assignment)
$consumer->subscribe(['email-validation']);

echo "Consumer started. Waiting for messages... (Ctrl+C to stop)" . PHP_EOL;
echo str_repeat('─', 60) . PHP_EOL;

while (true) {
    // CONCEPT: consume(timeout_ms) — blocks up to 5s waiting for a message.
    $message = $consumer->consume(5000);

    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            // A real message arrived — process it
            $payload = json_decode($message->payload, true);
            echo sprintf(
                "[MSG] partition=%d | offset=%d | event_id=%s | email=%s\n",
                $message->partition,
                $message->offset,
                $payload['event_id'] ?? 'unknown',
                $payload['email'] ?? 'unknown',
            );
            break;

        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            // CONCEPT: Timed out — no messages arrived within 5 seconds.
            // This is NORMAL. The consumer is idle, not broken.
            echo "[IDLE] No messages in last 5s, still listening..." . PHP_EOL;
            break;

        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            // CONCEPT: Reached the end of the partition.
            // The consumer has read all current messages.
            // New messages will arrive as producers write them.
            echo "[EOF] Reached end of partition {$message->partition}" . PHP_EOL;
            break;

        default:
            // A real error — log it but keep the loop running
            echo "[ERROR] " . $message->errstr() . PHP_EOL;
            break;
    }
}

function env_or(string $key, string $default): string {
    return getenv($key) ?: $default;
}
```

### Commands to run

```bash
# Terminal 1: start the consumer (keep it running)
docker exec -it laravel-app php scripts/consume-test.php

# Terminal 2: produce a message and watch Terminal 1
docker exec laravel-app php scripts/produce-test.php

# Check the consumer group was created
./kafka-scripts/kafka-inspect.sh list-groups
./kafka-scripts/kafka-inspect.sh describe-group email-validation-workers
```

### Expected output

```
# Consumer terminal:
Consumer started. Waiting for messages... (Ctrl+C to stop)
────────────────────────────────────────────────────────────
[IDLE] No messages in last 5s, still listening...
[MSG] partition=1 | offset=0 | event_id=4b2a... | email=test@example.com
[MSG] partition=0 | offset=0 | event_id=9c1f... | email=test@example.com

# describe-group:
GROUP                     PARTITION  CURRENT-OFFSET  LAG
email-validation-workers  0          1               0
email-validation-workers  1          1               0
email-validation-workers  2          0               0
```

### Common mistakes

| Mistake | Fix |
|---|---|
| Consumer prints nothing | Producer hasn't run yet, or `auto.offset.reset=latest` skips old messages |
| `_TIMED_OUT` treated as crash | It's normal; use a `switch` as shown — never crash on timeout |
| Consumer exits immediately | You need `while (true)` — a consumer is a long-running process |

---

## Phase 6 — Understand Partitions and Offsets

### Kafka concepts you will learn

**Partition Key:** A string attached to each message. Kafka hashes the key to determine which partition the message goes to. The same key always maps to the same partition (murmur2 hash % partition count). This guarantees ordering for all messages with the same key.

**Why use the email as the key?** All validation events for `user@example.com` will land on the same partition, in order. This means a consumer processing that partition sees events for that email in the exact sequence they were produced.

**Offset:** A monotonically increasing integer within each partition. Offset 0 is the first message, offset 1 is the second, etc. Offsets never reset (unless you explicitly do so). Offsets are per-partition — partition 0 has its own offset sequence independent of partition 1.

**Consumer Lag:** `lag = latest_offset - committed_offset`. Lag = 0 means the consumer is caught up. Lag > 0 means there are unread messages. Lag is the primary health metric for a Kafka consumer.

**What can go wrong:**
- Assuming ordering across partitions — Kafka only guarantees order within a single partition
- Changing the partition count after producing keyed messages — the same key will now route to a different partition, breaking ordering assumptions

### Steps

```bash
# 1. Produce messages WITH a key (email = partition key)
echo 'user@example.com:{"event_id":"aaa","event_type":"email.validation.requested","email":"user@example.com","requested_at":"2026-01-01T00:00:00Z"}' | \
  docker exec -i kafka kafka-console-producer \
    --bootstrap-server kafka:9092 \
    --topic email-validation \
    --property "parse.key=true" \
    --property "key.separator=:"

# Produce same key again — it MUST land on the same partition
echo 'user@example.com:{"event_id":"bbb","event_type":"email.validation.requested","email":"user@example.com","requested_at":"2026-01-01T00:00:01Z"}' | \
  docker exec -i kafka kafka-console-producer \
    --bootstrap-server kafka:9092 \
    --topic email-validation \
    --property "parse.key=true" \
    --property "key.separator=:"

# 2. Consume with key, partition, and offset visible
./kafka-scripts/kafka-inspect.sh consume-topic email-validation

# 3. Check current consumer lag
./kafka-scripts/kafka-inspect.sh describe-group email-validation-workers

# 4. Reset consumer offset to replay everything
./kafka-scripts/kafka-inspect.sh reset-offset email-validation-workers email-validation

# 5. Check lag again — observe it jumped above 0, then drops as consumer reads
./kafka-scripts/kafka-inspect.sh describe-group email-validation-workers
```

### Expected output — key observation

```
# Both messages with key 'user@example.com' appear in the SAME partition:
Partition: 2 | KEY: user@example.com | Offset: 0 | {"event_id":"aaa"...}
Partition: 2 | KEY: user@example.com | Offset: 1 | {"event_id":"bbb"...}

# describe-group after reset:
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
0          -               1               1
1          -               3               3
2          -               2               2   ← lag is now > 0
```

### Common mistakes

| Mistake | Fix |
|---|---|
| Two emails land on same partition — is that wrong? | No — different keys can hash to the same partition. Ordering is still per-key within the partition. |
| Offset reset doesn't work | Stop the consumer first; can't reset offsets of an active consumer group |
| Assuming offset = row number | Offsets are per-partition, not global |

---

## Phase 7 — Integrate Laravel API with Kafka Producer

### Kafka concepts you will learn

**Producer in a web request context:** Unlike the standalone script, the API must respond quickly. The producer call must have a strict timeout. If Kafka is unavailable, the API should return a graceful error — not hang.

**Event Schema:** We define a standard JSON envelope: `event_id`, `event_type`, `email`, `requested_at`. The `event_id` (UUID) is what we'll use for idempotency in Phase 11.

**Why `202 Accepted`?** The API publishes to Kafka and returns immediately. It does NOT wait for the email to be validated. The validation happens asynchronously. HTTP 202 communicates "I've accepted your request and will process it".

### Files created/modified

```
laravel/
├── app/
│   ├── Http/Controllers/Api/EmailValidationController.php
│   ├── Services/KafkaProducerService.php
│   └── Kafka/Events/EmailValidationRequested.php
├── routes/api.php                    (add route)
└── config/kafka.php                  (new config file)
```

**`config/kafka.php`**
```php
<?php
return [
    'brokers'          => env('KAFKA_BROKERS', 'kafka:9092'),
    'validation_topic' => env('KAFKA_EMAIL_VALIDATION_TOPIC', 'email-validation'),
    'dlq_topic'        => env('KAFKA_EMAIL_VALIDATION_DLQ_TOPIC', 'email-validation-dlq'),
    'consumer_group'   => env('KAFKA_CONSUMER_GROUP', 'email-validation-workers'),
    'producer_timeout' => (int) env('KAFKA_PRODUCER_TIMEOUT_MS', 5000),
];
```

**`app/Kafka/Events/EmailValidationRequested.php`**
```php
<?php
declare(strict_types=1);
namespace App\Kafka\Events;

use Ramsey\Uuid\Uuid;

final class EmailValidationRequested
{
    public readonly string $eventId;
    public readonly string $eventType;
    public readonly string $requestedAt;

    public function __construct(public readonly string $email)
    {
        $this->eventId     = Uuid::uuid4()->toString();
        $this->eventType   = 'email.validation.requested';
        $this->requestedAt = now()->toIso8601String();
    }

    public function toArray(): array
    {
        return [
            'event_id'     => $this->eventId,
            'event_type'   => $this->eventType,
            'email'        => $this->email,
            'requested_at' => $this->requestedAt,
        ];
    }

    /** The partition key — same email always routes to the same partition */
    public function partitionKey(): string
    {
        return $this->email;
    }
}
```

**`app/Services/KafkaProducerService.php`**
```php
<?php
declare(strict_types=1);
namespace App\Services;

use App\Kafka\Events\EmailValidationRequested;
use RdKafka\Conf;
use RdKafka\Producer;
use RuntimeException;

final class KafkaProducerService
{
    private Producer $producer;
    private string $topic;
    private int $timeoutMs;

    public function __construct()
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', config('kafka.brokers'));
        $conf->set('acks', '1');

        $this->producer  = new Producer($conf);
        $this->topic     = config('kafka.validation_topic');
        $this->timeoutMs = config('kafka.producer_timeout');
    }

    public function publish(EmailValidationRequested $event): void
    {
        $topic   = $this->producer->newTopic($this->topic);
        $payload = json_encode($event->toArray(), JSON_THROW_ON_ERROR);

        // CONCEPT: Partition key = email address.
        // All events for the same email land on the same partition (ordered).
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload, $event->partitionKey());

        // CONCEPT: flush() — wait for broker acknowledgement before returning.
        // This ensures the message is durably stored before we respond HTTP 202.
        $result = $this->producer->flush($this->timeoutMs);

        if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new RuntimeException("Kafka flush failed with code: {$result}");
        }
    }
}
```

**`app/Http/Controllers/Api/EmailValidationController.php`**
```php
<?php
declare(strict_types=1);
namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Kafka\Events\EmailValidationRequested;
use App\Services\KafkaProducerService;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;

final class EmailValidationController extends Controller
{
    public function __construct(private readonly KafkaProducerService $producer) {}

    public function store(Request $request): JsonResponse
    {
        $validated = $request->validate([
            'email' => ['required', 'string', 'max:255'],
        ]);

        $event = new EmailValidationRequested($validated['email']);

        $this->producer->publish($event);

        // HTTP 202 Accepted: "I received your request and will process it asynchronously"
        return response()->json([
            'event_id' => $event->eventId,
            'status'   => 'queued',
        ], 202);
    }
}
```

**`routes/api.php`** — add:
```php
Route::post('/email-validations', [EmailValidationController::class, 'store']);
```

### Commands to run

```bash
# Rebuild the app container (new config file added)
docker compose build app
docker compose up -d app

# Test the API
curl -X POST http://localhost:8000/api/email-validations \
  -H "Content-Type: application/json" \
  -d '{"email": "hello@example.com"}'
```

### Expected output

```json
{
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "queued"
}
```

### Common mistakes

| Mistake | Fix |
|---|---|
| `flush()` hangs forever | Set a timeout (5000ms); catch the error and return HTTP 503 |
| Kafka unavailable → 500 error | Wrap publish in try/catch; return 503 Service Unavailable |
| Using Laravel's `ramsey/uuid` | It's already a Laravel 13 dependency via `illuminate/support` |

---

## Phase 8 — Build the Email-Validation Consumer Command

### Kafka concepts you will learn

**Long-running consumer as an Artisan command:** Web workers handle HTTP requests. Kafka consumers are daemons — they run indefinitely in a loop. An Artisan command with `php artisan consume:email-validations` is the right pattern. In production, Supervisor manages these processes.

**Signal handling:** The consumer should stop gracefully on `SIGTERM` (Docker stop, Supervisor shutdown). Without this, the process is killed mid-message, leaving the offset uncommitted.

### Files created

`app/Console/Commands/ConsumeEmailValidations.php`

```php
<?php
declare(strict_types=1);
namespace App\Console\Commands;

use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

final class ConsumeEmailValidations extends Command
{
    protected $signature   = 'consume:email-validations {--worker-id=1 : Worker identifier for logging}';
    protected $description = 'Consume and validate email events from Kafka';

    private bool $running = true;

    public function handle(): int
    {
        $workerId = $this->option('worker-id');
        $this->info("Worker [{$workerId}] starting...");

        // CONCEPT: Signal handling — stop gracefully on SIGTERM
        pcntl_signal(SIGTERM, fn() => $this->running = false);
        pcntl_signal(SIGINT,  fn() => $this->running = false);

        $consumer = $this->buildConsumer();
        $consumer->subscribe([config('kafka.validation_topic')]);

        $this->info("Worker [{$workerId}] listening for messages. Ctrl+C to stop.");

        while ($this->running) {
            pcntl_signal_dispatch(); // process pending signals

            $message = $consumer->consume(5000);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->processMessage($message, $workerId);
                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // Normal — no messages in 5s window
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    // Normal — caught up to end of partition
                    break;

                default:
                    $this->error("[{$workerId}] Consumer error: " . $message->errstr());
                    break;
            }
        }

        $this->info("Worker [{$workerId}] shutting down gracefully.");
        return Command::SUCCESS;
    }

    private function processMessage(\RdKafka\Message $message, string $workerId): void
    {
        $payload = json_decode($message->payload, true);
        $eventId = $payload['event_id'] ?? 'unknown';
        $email   = $payload['email'] ?? '';

        $this->info(sprintf(
            "[%s] partition=%d | offset=%d | event_id=%s | email=%s",
            $workerId,
            $message->partition,
            $message->offset,
            $eventId,
            $email,
        ));

        // Validate the email (simple — DNS check added in Phase 8 full impl)
        $isValid = filter_var($email, FILTER_VALIDATE_EMAIL) !== false;
        $this->info("[{$workerId}] Result: " . ($isValid ? 'VALID' : 'INVALID'));
    }

    private function buildConsumer(): KafkaConsumer
    {
        $conf = new Conf();
        $conf->set('group.id', config('kafka.consumer_group'));
        $conf->set('metadata.broker.list', config('kafka.brokers'));
        $conf->set('auto.offset.reset', 'earliest');
        // Note: auto-commit stays ON here. Disabled in Phase 10.
        $conf->set('enable.auto.commit', 'true');
        return new KafkaConsumer($conf);
    }
}
```

### Commands to run

```bash
# Run the consumer
docker exec -it laravel-app php artisan consume:email-validations --worker-id=1

# In another terminal — produce a message via the API
curl -X POST http://localhost:8000/api/email-validations \
  -H "Content-Type: application/json" \
  -d '{"email": "hello@example.com"}'
```

### Expected output

```
Worker [1] starting...
Worker [1] listening for messages. Ctrl+C to stop.
[1] partition=0 | offset=5 | event_id=550e8400... | email=hello@example.com
[1] Result: VALID
```

---

## Phase 9 — Persist Results in PostgreSQL

### What you will learn
How to store consumer processing results durably. The DB write and the Kafka offset commit must be atomic from the application's perspective — if the DB write fails, we must not commit the offset (so we can retry the message).

### Files created

```bash
# Create migration
docker exec laravel-app php artisan make:migration create_email_validations_table
docker exec laravel-app php artisan make:model EmailValidation
```

**Migration:**
```php
Schema::create('email_validations', function (Blueprint $table) {
    $table->uuid('id')->primary();
    $table->string('event_id')->unique();     // idempotency key (Phase 11)
    $table->string('email');
    $table->enum('status', ['queued', 'valid', 'invalid', 'failed', 'dead_lettered']);
    $table->text('error_message')->nullable();
    $table->integer('attempt')->default(1);
    $table->timestamp('validated_at')->nullable();
    $table->timestamps();
});
```

```bash
docker exec laravel-app php artisan migrate
```

**Update `ConsumeEmailValidations::processMessage()`** to write to DB after validation.

---

## Phase 10 — Manual Offset Commits

### What you will learn
**The critical difference:** Auto-commit commits the offset on a timer, regardless of whether processing succeeded. If the consumer crashes after auto-commit but before the DB write, the message is lost forever (at-most-once). Manual commit lets you commit only after a confirmed DB write (at-least-once).

```php
// Disable auto-commit
$conf->set('enable.auto.commit', 'false');

// In processMessage() — commit ONLY after successful DB write
try {
    $this->saveToDatabase($payload);
    $consumer->commit($message); // CONCEPT: commit after successful processing
} catch (\Exception $e) {
    // Do NOT commit — message will be re-delivered
    $this->error("Processing failed, offset NOT committed: " . $e->getMessage());
}
```

---

## Phase 11 — Idempotency

### What you will learn
At-least-once delivery means the same message can arrive twice (crash after DB write, before offset commit → message replayed on restart). Idempotency ensures processing the same message twice has the same effect as processing it once.

**Strategy:** Use `event_id` as a unique database key. Before processing, check if it already exists.

```php
// At the start of processMessage()
if (EmailValidation::where('event_id', $eventId)->exists()) {
    $this->info("[SKIP] event_id={$eventId} already processed (duplicate delivery)");
    $consumer->commit($message); // commit so we don't keep seeing it
    return;
}
```

---

## Phase 12 — Retry Handling

### What you will learn
Not all failures are permanent. A DNS timeout is transient (retry makes sense). An invalid email format is permanent (retrying won't fix it — send straight to DLQ).

```
Processing attempt 1 → fails (transient) → wait 2s
Processing attempt 2 → fails (transient) → wait 4s
Processing attempt 3 → fails (transient) → publish to DLQ, commit offset
```

```php
$maxAttempts = 3;
for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
    try {
        $this->validateAndSave($payload);
        $consumer->commit($message);
        return; // success
    } catch (TransientException $e) {
        if ($attempt === $maxAttempts) {
            $this->publishToDlq($message, $e); // Phase 13
            $consumer->commit($message);
            return;
        }
        sleep(2 ** $attempt); // exponential backoff: 2s, 4s, 8s
    } catch (PermanentException $e) {
        $this->publishToDlq($message, $e);
        $consumer->commit($message);
        return;
    }
}
```

---

## Phase 13 — Dead Letter Queue

### What you will learn
The DLQ is just another Kafka topic. Failed messages are published there with enriched metadata (original topic, partition, offset, error reason) so they can be investigated and replayed later without blocking the main topic.

**DLQ message envelope:**
```json
{
    "original_topic": "email-validation",
    "original_partition": 1,
    "original_offset": 42,
    "error": "DNS lookup failed after 3 attempts",
    "attempts": 3,
    "failed_at": "2026-01-01T00:00:00Z",
    "original_payload": { ... }
}
```

```bash
# Inspect DLQ messages
./kafka-scripts/kafka-inspect.sh consume-topic email-validation-dlq
```

---

## Phase 14 — Multiple Consumers & Consumer Groups

### What you will learn
Kafka assigns each partition to exactly one consumer within a group. With 3 partitions and 2 consumers, one consumer gets 2 partitions and the other gets 1. A third consumer gets one each. A fourth consumer sits idle (no partition to assign).

```bash
# Terminal 1
docker exec -it laravel-app php artisan consume:email-validations --worker-id=1

# Terminal 2
docker exec -it laravel-app php artisan consume:email-validations --worker-id=2

# Observe partition assignment
./kafka-scripts/kafka-inspect.sh describe-group email-validation-workers

# Second independent group (separate offset tracking)
docker exec -it laravel-app \
  KAFKA_CONSUMER_GROUP=analytics-workers \
  php artisan consume:email-validations --worker-id=analytics-1
```

---

## Phase 15 — Consumer Failure & Rebalancing

### What you will learn
When a consumer stops sending heartbeats for longer than `session.timeout.ms` (default 45s), Kafka marks it dead and triggers a **rebalance** — redistributing its partitions among surviving consumers. The surviving consumers resume from the last committed offset of the dead consumer's partitions.

```bash
# Start 2 consumers, then kill one with Ctrl+C
# Watch the other consumer adopt the orphaned partition
./kafka-scripts/kafka-inspect.sh describe-group email-validation-workers
```

---

## Phase 16 — Structured Logging & Metrics

### What you will learn
Structured logs (JSON key-value pairs) are machine-parseable by log aggregators (ELK, Loki, Datadog). Consumer lag is the primary health metric — monitor it in production.

```php
Log::info('email.validation.processed', [
    'worker_id'    => $this->workerId,
    'event_id'     => $eventId,
    'email'        => $email,
    'status'       => $status,
    'partition'    => $message->partition,
    'offset'       => $message->offset,
    'attempt'      => $attempt,
    'latency_ms'   => $latencyMs,
]);
```

Add `GET /api/metrics` endpoint returning processed count, valid/invalid counts, DLQ count.

---

## Phase 17 — Performance Testing

### What you will learn
Throughput = messages/second. The bottleneck is usually the slowest step: DNS lookup (network I/O), DB write (disk I/O), or PHP overhead. Consumer lag growing faster than it shrinks = throughput problem.

```bash
# Bulk produce 1000 messages
docker exec laravel-app php scripts/bulk-produce.php --count=1000

# Watch lag decrease in real time
watch -n 1 './kafka-scripts/kafka-inspect.sh describe-group email-validation-workers'
```

---

## Phase 18 — Final Cleanup & Documentation

1. Remove `scripts/produce-test.php`, `scripts/consume-test.php` (replaced by proper commands)
2. Ensure all config values use environment variables
3. Create `docs/architecture.md` — system diagram and data flow
4. Create `docs/kafka-concepts.md` — glossary with project examples
5. Create `docs/runbook.md` — how to operate, inspect, drain DLQ, replay offsets
6. Update `README.md` — mark all phases complete
7. Full reset test: `docker compose down -v && docker compose up -d`

---

## Final File Structure

```
kafka-email-validation/
├── docker-compose.yml
├── .env
├── README.md
├── plan.md
├── docker/app/Dockerfile
├── docker/postgres/init.sql
├── kafka-scripts/kafka-inspect.sh
├── docs/
│   ├── architecture.md
│   ├── kafka-concepts.md
│   └── runbook.md
└── laravel/
    ├── config/kafka.php
    ├── routes/api.php
    ├── app/
    │   ├── Kafka/Events/EmailValidationRequested.php
    │   ├── Services/KafkaProducerService.php
    │   ├── Services/KafkaDlqPublisherService.php
    │   ├── Http/Controllers/Api/EmailValidationController.php
    │   ├── Console/Commands/ConsumeEmailValidations.php
    │   └── Models/EmailValidation.php
    ├── database/migrations/
    │   └── ..._create_email_validations_table.php
    └── scripts/
        └── bulk-produce.php       (Phase 17, removed in Phase 18)
```

---

## Kafka Concepts Reference

| Concept | One-line definition |
|---|---|
| **Broker** | The Kafka server — stores messages, serves producers and consumers |
| **Topic** | Named append-only log; reading doesn't remove messages |
| **Partition** | Ordered sub-log of a topic; unit of parallelism |
| **Message** | Key + Value + Headers + Metadata (partition, offset, timestamp) |
| **Offset** | Position of a message within a partition (never resets) |
| **Producer** | Client that writes messages to a topic |
| **Consumer** | Client that reads messages from a topic |
| **Consumer Group** | Set of consumers sharing partition assignment |
| **Offset Commit** | Saving "I have processed up to offset N" for a group |
| **Consumer Lag** | `latest_offset - committed_offset`; primary health metric |
| **Partition Key** | Determines which partition a message goes to (hash-based) |
| **Rebalancing** | Redistribution of partitions when group membership changes |
| **At-most-once** | Commit before processing — fast, may lose messages |
| **At-least-once** | Commit after processing — safe, may duplicate on crash |
| **Idempotency** | Processing the same message twice = same result (use `event_id`) |
| **Retry** | Re-attempting failed processing before giving up |
| **DLQ** | Topic for messages that failed all retries — inspect without blocking main topic |
