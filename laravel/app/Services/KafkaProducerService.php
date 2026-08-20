<?php
declare(strict_types=1);

namespace App\Services;

use App\Kafka\Events\EmailValidationRequested;
use RdKafka\Conf;
use RdKafka\Producer;
use RuntimeException;

class KafkaProducerService
{
    private Producer $producer;
    private string $topic;
    private int $timeoutMs;

    public function __construct()
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', config('kafka.brokers'));
        $conf->set('acks', '1');

        $this->producer = new Producer($conf);
        $this->topic = config('kafka.validation_topic');
        $this->timeoutMs = config('kafka.producer_timeout');
    }

    public function publish(EmailValidationRequested $event): void
    {
        $topic = $this->producer->newTopic($this->topic);
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
