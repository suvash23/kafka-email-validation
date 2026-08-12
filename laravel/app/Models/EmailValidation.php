<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Concerns\HasUuids;
use Illuminate\Database\Eloquent\Model;

class EmailValidation extends Model
{
    use HasUuids;

    protected $fillable = [
        'event_id',
        'email',
        'status',
        'error_message',
        'attempt',
        'validated_at',
    ];

    protected $casts = [
        'validated_at' => 'datetime',
        'attempt' => 'integer',
    ];
}
