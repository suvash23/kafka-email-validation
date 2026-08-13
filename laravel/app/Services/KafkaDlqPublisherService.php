<?php
declare(strict_types=1);

namespace App\Services;

use RdKafka\Conf;
use RdKafka\Producer;
use RuntimeException;

final class KafkaDlqPublisherService
{
    private Producer $producer;
    private string $dlqTopic;
    private int $timeoutMs;

    public function __construct()
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', config('kafka.brokers'));
        $conf->set('acks', '1');

        $this->producer = new Producer($conf);
        $this->dlqTopic = config('kafka.dlq_topic');
        $this->timeoutMs = config('kafka.producer_timeout', 5000);
    }

    public function publish(\RdKafka\Message $message, \Exception $e, int $attempts = 1): void
    {
        $topic = $this->producer->newTopic($this->dlqTopic);

        $payload = json_decode($message->payload, true);

        // DLQ message envelope
        $envelope = [
            'original_topic' => $message->topic_name,
            'original_partition' => $message->partition,
            'original_offset' => $message->offset,
            'error' => $e->getMessage(),
            'attempts' => $attempts,
            'failed_at' => now()->toIso8601String(),
            'original_payload' => $payload,
        ];

        $jsonPayload = json_encode($envelope, JSON_THROW_ON_ERROR);

        $partitionKey = $payload['email'] ?? null;

        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $jsonPayload, $partitionKey);

        $result = $this->producer->flush($this->timeoutMs);

        if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new RuntimeException("Kafka DLQ flush failed with code: {$result}");
        }
    }
}
