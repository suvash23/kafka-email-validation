<?php
declare(strict_types=1);

namespace Tests\Unit\Kafka;

use App\Kafka\Events\EmailValidationRequested;
use PHPUnit\Framework\TestCase;

class EmailValidationRequestedTest extends TestCase
{
    public function test_it_generates_valid_payload_and_uses_email_as_partition_key()
    {
        $email = 'test@example.com';
        $event = new EmailValidationRequested($email);

        // Ensure the partition key is the email for sequential routing
        $this->assertEquals($email, $event->partitionKey());

        $array = $event->toArray();
        $this->assertEquals($email, $array['email']);
        $this->assertEquals('email.validation.requested', $array['event_type']);
        $this->assertArrayHasKey('event_id', $array);
        $this->assertArrayHasKey('requested_at', $array);
    }
}
