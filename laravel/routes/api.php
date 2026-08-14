<?php

use App\Http\Controllers\Api\EmailValidationController;
use App\Http\Controllers\Api\MetricsController;
use Illuminate\Support\Facades\Route;

Route::post('/email-validations', [EmailValidationController::class, 'store']);
Route::get('/metrics', [MetricsController::class, 'index']);
