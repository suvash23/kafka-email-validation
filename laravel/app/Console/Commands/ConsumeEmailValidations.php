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

    public function __construct(private \App\Services\KafkaDlqPublisherService $dlqPublisher)
    {
        parent::__construct();
    }

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
                    $this->processMessage($message, (string) $workerId, $consumer);
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

    private function processMessage(\RdKafka\Message $message, string $workerId, KafkaConsumer $consumer): void
    {
        $payload = json_decode($message->payload, true);
        $eventId = $payload['event_id'] ?? 'unknown';
        $email = $payload['email'] ?? '';

        if (EmailValidation::where('event_id', $eventId)->exists()) {
            $this->info("[SKIP] event_id={$eventId} already processed (duplicate delivery)");
            $consumer->commit($message); // commit so we don't keep seeing it
            return;
        }

        $this->info(sprintf(
            "[%s] partition=%d | offset=%d | event_id=%s | email=%s",
            $workerId,
            $message->partition,
            $message->offset,
            $eventId,
            $email,
        ));

        $maxAttempts = 3;
        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            try {
                $this->validateAndSave($payload, $workerId);
                $consumer->commit($message);
                return; // success
            } catch (\App\Exceptions\TransientException $e) {
                if ($attempt === $maxAttempts) {
                    $this->publishToDlq($message, $e); // Phase 13
                    $consumer->commit($message);
                    return;
                }
                $this->warn("[Worker {$workerId}] Transient error: " . $e->getMessage() . " - retrying (attempt {$attempt})");
                sleep(2 ** $attempt); // exponential backoff: 2s, 4s, 8s
            } catch (\App\Exceptions\PermanentException $e) {
                $this->error("[Worker {$workerId}] Permanent error: " . $e->getMessage());
                $this->publishToDlq($message, $e);
                $consumer->commit($message);
                return;
            } catch (\Exception $e) {
                $this->error("[Worker {$workerId}] Unexpected error: " . $e->getMessage());
                // For fully unexpected errors, don't commit (re-deliver on restart)
                return;
            }
        }

        $this->info(str_repeat('-', 20));
    }

    private function validateAndSave(array $payload, string $workerId): void
    {
        $eventId = $payload['event_id'] ?? 'unknown';
        $email = $payload['email'] ?? '';

        $isValid = filter_var($email, FILTER_VALIDATE_EMAIL) !== false;

        EmailValidation::create([
            'event_id' => $eventId,
            'email' => $email,
            'status' => $isValid ? 'valid' : 'invalid',
            'validated_at' => now(),
        ]);

        $this->info("[Worker {$workerId}] Saved to DB: " . ($isValid ? 'VALID' : 'INVALID'));
    }

    private function publishToDlq(\RdKafka\Message $message, \Exception $e, int $attempts = 1): void
    {
        $this->dlqPublisher->publish($message, $e, $attempts);
        $this->error("Sent to DLQ: " . $e->getMessage());
    }

    private function buildConsumer(): KafkaConsumer
    {
        $conf = new Conf();
        $conf->set('group.id', config('kafka.consumer_group'));
        $conf->set('metadata.broker.list', config('kafka.brokers'));

        // Start from earliest un-committed message to replay what we've already done!
        $conf->set('auto.offset.reset', 'earliest');

        // Disable auto-commit for Phase 10.
        $conf->set('enable.auto.commit', 'false');

        return new KafkaConsumer($conf);
    }
}
