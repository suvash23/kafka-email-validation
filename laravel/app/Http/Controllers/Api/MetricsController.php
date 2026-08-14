<?php
declare(strict_types=1);

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\EmailValidation;
use Illuminate\Http\JsonResponse;

final class MetricsController extends Controller
{
    public function index(): JsonResponse
    {
        $counts = EmailValidation::toBase()
            ->selectRaw('status, COUNT(*) as count')
            ->groupBy('status')
            ->pluck('count', 'status');

        $total = $counts->sum();

        return response()->json([
            'total_processed' => $total,
            'valid' => (int) ($counts['valid'] ?? 0),
            'invalid' => (int) ($counts['invalid'] ?? 0),
            'failed' => (int) ($counts['failed'] ?? 0),
            'dead_lettered' => (int) ($counts['dead_lettered'] ?? 0),
            'queued' => (int) ($counts['queued'] ?? 0),
        ]);
    }
}
