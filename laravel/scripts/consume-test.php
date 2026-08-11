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
 *   sudo docker exec -it laravel-app php scripts/consume-test.php
 *
 * Stop with Ctrl+C.
 */

$conf = new RdKafka\Conf();

// CONCEPT: group.id — identifies this consumer's group.
// Kafka tracks committed offsets per (group + topic + partition).
$conf->set('group.id', env_or('KAFKA_CONSUMER_GROUP', 'email-validation-workers'));

// The broker address (inside Docker network)
$conf->set('metadata.broker.list', env_or('KAFKA_BROKERS', 'kafka:9092'));

// CONCEPT: auto.offset.reset
// 'earliest' → replay all messages from the beginning of the partition (if no committed offset exists).
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
            // Optionally, break or exit depending on the error severity
            break;
    }
}

function env_or(string $key, string $default): string
{
    return getenv($key) ?: $default;
}
