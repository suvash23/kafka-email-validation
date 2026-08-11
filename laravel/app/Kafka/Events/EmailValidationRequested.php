<?php
declare(strict_types=1);

namespace App\Kafka\Events;

use Illuminate\Support\Str;

final class EmailValidationRequested
{
    public readonly string $eventId;
    public readonly string $eventType;
    public readonly string $requestedAt;

    public function __construct(public readonly string $email)
    {
        $this->eventId = Str::uuid()->toString();
        $this->eventType = 'email.validation.requested';
        $this->requestedAt = now()->toIso8601String();
    }

    public function toArray(): array
    {
        return [
            'event_id' => $this->eventId,
            'event_type' => $this->eventType,
            'email' => $this->email,
            'requested_at' => $this->requestedAt,
        ];
    }

    /**
     * The partition key — same email always routes to the same partition
     */
    public function partitionKey(): string
    {
        return $this->email;
    }
}
