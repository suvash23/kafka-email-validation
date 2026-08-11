<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class EmailValidation extends Model
{
    /**
     * Disable default integer ID in favor of our UUID primary key.
     */
    public $incrementing = false;
    protected $primaryKey = 'id';
    protected $keyType = 'string';

    protected $fillable = [
        'id',
        'email',
        'is_valid',
        'raw_event_payload',
        'partition',
        'offset',
    ];

    protected $casts = [
        'is_valid' => 'boolean',
        'raw_event_payload' => 'array',
    ];
}
