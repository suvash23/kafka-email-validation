<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('email_validations', function (Blueprint $table) {
            $table->uuid('id')->primary();
            $table->string('event_id')->unique();     // idempotency key (Phase 11)
            $table->string('email');
            $table->enum('status', ['queued', 'valid', 'invalid', 'failed', 'dead_lettered']);
            $table->text('error_message')->nullable();
            $table->integer('attempt')->default(1);
            $table->timestamp('validated_at')->nullable();
            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('email_validations');
    }
};
