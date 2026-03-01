<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $out = [];

        $handle = fopen($inputPath, 'r');
        if ($handle === false) {
            throw new Exception("Failed to open input file: {$inputPath}");
        }

        try {
            while (($line = fgets($handle)) !== false) {
                $line = rtrim($line, "\r\n");
                if ($line !== '') {
                    [$path, $date] = $this->parseLine($line);
                    if (!isset($out[$path])) $out[$path] = [];
                    $out[$path][$date] = ($out[$path][$date] ?? 0) + 1;
                }
            }
        } finally {
            fclose($handle);
        }

        foreach ($out as $path => &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($out, JSON_PRETTY_PRINT));
    }

    private function parseLine(string $line): array
    {
        // skipping https://stitcher.io/, , it's all the same domain anyway
        // also skipping 15ch of datetime
        $line = substr($line, 19, -15);

        return explode(',', $line);
    }
}