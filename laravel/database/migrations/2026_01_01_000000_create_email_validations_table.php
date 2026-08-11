<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('email_validations', function (Blueprint $table) {
            $table->uuid('id')->primary(); // Will store the event_id
            $table->string('email')->index();
            $table->boolean('is_valid');
            $table->jsonb('raw_event_payload');

            // Helpful for debugging Kafka offset mapping
            $table->integer('partition');
            $table->bigInteger('offset');

            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('email_validations');
    }
};
