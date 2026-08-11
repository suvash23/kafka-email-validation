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
 *   sudo docker exec laravel-app php scripts/produce-test.php
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
    'event_id' => sprintf(
        '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
        mt_rand(0, 0xffff),
        mt_rand(0, 0xffff),
        mt_rand(0, 0xffff),
        mt_rand(0, 0x0fff) | 0x4000,
        mt_rand(0, 0x3fff) | 0x8000,
        mt_rand(0, 0xffff),
        mt_rand(0, 0xffff),
        mt_rand(0, 0xffff)
    ),
    'event_type' => 'email.validation.requested',
    'email' => 'test@example.com',
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

function env_or(string $key, string $default): string
{
    return getenv($key) ?: $default;
}
