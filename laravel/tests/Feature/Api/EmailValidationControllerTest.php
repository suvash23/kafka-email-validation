<?php
declare(strict_types=1);

namespace Tests\Feature\Api;

use App\Kafka\Events\EmailValidationRequested;
use App\Services\KafkaProducerService;
use Mockery;
use Mockery\MockInterface;
use Tests\TestCase;

class EmailValidationControllerTest extends TestCase
{
    public function test_it_dispatches_kafka_event_and_returns_202()
    {
        // Mock the KafkaProducerService
        $this->instance(
            KafkaProducerService::class,
            Mockery::mock(KafkaProducerService::class, function (MockInterface $mock) {
                // Ensure publish is called once with an event containing the correct email
                $mock->shouldReceive('publish')
                    ->once()
                    ->withArgs(function (EmailValidationRequested $event) {
                    return $event->email === 'happy.path@example.com';
                });
            })
        );

        $response = $this->postJson('/api/email-validations', [
            'email' => 'happy.path@example.com'
        ]);

        $response->assertStatus(202)
            ->assertJsonStructure([
                'event_id',
                'status'
            ])
            ->assertJsonFragment([
                'status' => 'queued'
            ]);
    }

    public function test_it_requires_a_valid_email()
    {
        $response = $this->postJson('/api/email-validations', [
            'email' => 'not-an-email'
        ]);

        $response->assertStatus(422)
            ->assertJsonValidationErrors(['email']);
    }
}
