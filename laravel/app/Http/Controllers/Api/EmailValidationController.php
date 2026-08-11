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
    public function __construct(private readonly KafkaProducerService $producer)
    {
    }

    public function store(Request $request): JsonResponse
    {
        $validated = $request->validate([
            'email' => ['required', 'string', 'email', 'max:255'],
        ]);

        $event = new EmailValidationRequested($validated['email']);

        // Publish to Kafka. This will block briefly until Kafka acknowledges to ensure delivery.
        $this->producer->publish($event);

        // HTTP 202 Accepted: "I received your request and will process it asynchronously"
        return response()->json([
            'event_id' => $event->eventId,
            'status' => 'queued',
        ], 202);
    }
}
