<?php
declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\EmailValidation;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
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

        Log::info('consumer.started', ['worker_id' => $workerId]);
        $this->info("Worker [{$workerId}] starting...");

        pcntl_signal(SIGTERM, fn() => $this->running = false);
        pcntl_signal(SIGINT, fn() => $this->running = false);

        $consumer = $this->buildConsumer();
        $consumer->subscribe([config('kafka.validation_topic')]);

        $this->info("Worker [{$workerId}] listening for messages. Ctrl+C to stop.");
        $this->info(str_repeat('─', 60));

        while ($this->running) {
            pcntl_signal_dispatch();

            $message = $consumer->consume(5000);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->processMessage($message, (string) $workerId, $consumer);
                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    break;

                default:
                    Log::error('consumer.error', [
                        'worker_id' => $workerId,
                        'error' => $message->errstr(),
                    ]);
                    $this->error("[Worker {$workerId}] Consumer error: " . $message->errstr());
                    break;
            }
        }

        Log::info('consumer.stopped', ['worker_id' => $workerId]);
        $this->info("Worker [{$workerId}] shutting down gracefully.");
        return Command::SUCCESS;
    }

    private function processMessage(\RdKafka\Message $message, string $workerId, KafkaConsumer $consumer): void
    {
        $startedAt = microtime(true);
        $payload = json_decode($message->payload, true);
        $eventId = $payload['event_id'] ?? 'unknown';
        $email = $payload['email'] ?? '';

        if (EmailValidation::where('event_id', $eventId)->exists()) {
            Log::info('email.validation.skipped', [
                'worker_id' => $workerId,
                'event_id' => $eventId,
                'reason' => 'duplicate_delivery',
                'partition' => $message->partition,
                'offset' => $message->offset,
            ]);
            $this->info("[SKIP] event_id={$eventId} already processed (duplicate delivery)");
            $consumer->commit($message);
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

                $latencyMs = (int) ((microtime(true) - $startedAt) * 1000);
                Log::info('email.validation.processed', [
                    'worker_id' => $workerId,
                    'event_id' => $eventId,
                    'email' => $email,
                    'partition' => $message->partition,
                    'offset' => $message->offset,
                    'attempt' => $attempt,
                    'latency_ms' => $latencyMs,
                ]);
                return;
            } catch (\App\Exceptions\TransientException $e) {
                Log::warning('email.validation.transient_error', [
                    'worker_id' => $workerId,
                    'event_id' => $eventId,
                    'attempt' => $attempt,
                    'error' => $e->getMessage(),
                ]);
                if ($attempt === $maxAttempts) {
                    $this->publishToDlq($message, $e, $attempt);
                    $consumer->commit($message);
                    return;
                }
                $this->warn("[Worker {$workerId}] Transient error: " . $e->getMessage() . " - retrying (attempt {$attempt})");
                sleep(2 ** $attempt);
            } catch (\App\Exceptions\PermanentException $e) {
                Log::error('email.validation.permanent_error', [
                    'worker_id' => $workerId,
                    'event_id' => $eventId,
                    'error' => $e->getMessage(),
                ]);
                $this->error("[Worker {$workerId}] Permanent error: " . $e->getMessage());
                $this->publishToDlq($message, $e, $attempt);
                $consumer->commit($message);
                return;
            } catch (\Exception $e) {
                Log::error('email.validation.unexpected_error', [
                    'worker_id' => $workerId,
                    'event_id' => $eventId,
                    'error' => $e->getMessage(),
                ]);
                $this->error("[Worker {$workerId}] Unexpected error: " . $e->getMessage());
                return;
            }
        }
    }

    private function validateAndSave(array $payload, string $workerId): void
    {
        $eventId = $payload['event_id'] ?? 'unknown';
        $email = $payload['email'] ?? '';
        $status = filter_var($email, FILTER_VALIDATE_EMAIL) !== false ? 'valid' : 'invalid';

        EmailValidation::create([
            'event_id' => $eventId,
            'email' => $email,
            'status' => $status,
            'validated_at' => now(),
        ]);

        $this->info("[Worker {$workerId}] Saved to DB: " . strtoupper($status));
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
