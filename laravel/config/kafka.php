<?php

return [
    'brokers' => env('KAFKA_BROKERS', 'kafka:9092'),
    'validation_topic' => env('KAFKA_EMAIL_VALIDATION_TOPIC', 'email-validation'),
    'dlq_topic' => env('KAFKA_EMAIL_VALIDATION_DLQ_TOPIC', 'email-validation-dlq'),
    'consumer_group' => env('KAFKA_CONSUMER_GROUP', 'email-validation-workers'),
    'producer_timeout' => (int) env('KAFKA_PRODUCER_TIMEOUT_MS', 5000),
];
