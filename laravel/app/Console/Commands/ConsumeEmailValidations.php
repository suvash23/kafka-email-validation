<?php
declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\EmailValidation;
use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

final class ConsumeEmailValidations extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'consume:email-validations {--worker-id=1 : Worker identifier for logging}';

    /**
     * The console command description.
     */
    protected $description = 'Consume and validate email events from Kafka';

    /**
     * Used to elegantly shut down the consumer daemon.
     */
    private bool $running = true;

    public function handle(): int
    {
        $workerId = $this->option('worker-id');
        $this->info("Worker [{$workerId}] starting...");

        // CONCEPT: Signal handling — stop gracefully on SIGTERM (e.g., when Docker stops the container)
        // If we don't handle signals, the process gets killed instantly, leaving the offset uncommitted.
        // `pcntl` extension is required (which we installed in Phase 1).
        pcntl_signal(SIGTERM, fn() => $this->running = false);
        pcntl_signal(SIGINT, fn() => $this->running = false);

        $consumer = $this->buildConsumer();

        // Subscribe to the topic. Unlike the producer, consumers must subscribe to a topic specifically.
        $consumer->subscribe([config('kafka.validation_topic')]);

        $this->info("Worker [{$workerId}] listening for messages. Ctrl+C to stop.");
        $this->info(str_repeat('─', 60));

        // Consumers run indefinitely in a loop.
        while ($this->running) {
            // Process any pending POSIX signals
            pcntl_signal_dispatch();

            // Wait up to 5 seconds for a new message
            $message = $consumer->consume(5000);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->processMessage($message, (string) $workerId);
                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // Normal idle state
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    // Normal end of partition block
                    break;

                default:
                    // Log the error but don't inherently crash the process
                    $this->error("[Worker {$workerId}] Consumer error: " . $message->errstr());
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
        $email = $payload['email'] ?? '';

        $this->info(sprintf(
            "[%s] partition=%d | offset=%d | event_id=%s | email=%s",
            $workerId,
            $message->partition,
            $message->offset,
            $eventId,
            $email,
        ));

        // Simplified Validation: Basic syntax check
        // In reality, this would perform a heavy DNS/MX records check or external API call.
        $isValid = filter_var($email, FILTER_VALIDATE_EMAIL) !== false;

        EmailValidation::create([
            'id' => $eventId,
            'email' => $email,
            'is_valid' => $isValid,
            'raw_event_payload' => $payload,
            'partition' => $message->partition,
            'offset' => $message->offset,
        ]);

        $this->info("[Worker {$workerId}] Saved to DB: " . ($isValid ? 'VALID' : 'INVALID'));
        $this->info(str_repeat('-', 20));
    }

    private function buildConsumer(): KafkaConsumer
    {
        $conf = new Conf();
        $conf->set('group.id', config('kafka.consumer_group'));
        $conf->set('metadata.broker.list', config('kafka.brokers'));

        // Start from earliest un-committed message to replay what we've already done!
        $conf->set('auto.offset.reset', 'earliest');

        // Let's leave auto-commit ON for now. We will learn to disable this in Phase 10 natively.
        $conf->set('enable.auto.commit', 'true');

        return new KafkaConsumer($conf);
    }
}
