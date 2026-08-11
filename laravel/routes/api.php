<?php

use App\Http\Controllers\Api\EmailValidationController;
use Illuminate\Support\Facades\Route;

Route::post('/email-validations', [EmailValidationController::class, 'store']);
