<?php

namespace App;

use Exception;

final class Parser
{
    private const int READ_CHUNK = 163_840;
    private const int WRITE_BUF = 1_048_576;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);
        
        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $out = [];

        $handle = fopen($inputPath, 'rb');
        if ($handle === false) {
            throw new Exception("Failed to open input file: {$inputPath}");
        }
        
        stream_set_read_buffer($handle, 0);

        try {
            $remaining = $fileSize;
            
            while ($remaining > 0) {
                $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
                $chunk = fread($handle, $toRead);
                $chunkLen = strlen($chunk);
                
                if ($chunkLen === 0) break;
                $remaining -= $chunkLen;
                
                $lastNl = strrpos($chunk, "\n");
                if ($lastNl === false) {
                    $this->processChunk($chunk, $dateIds, $out);
                    continue;
                }
                
                $this->processChunk(substr($chunk, 0, $lastNl + 1), $dateIds, $out);
                
                $tail = $chunkLen - $lastNl - 1;
                if ($tail > 0) {
                    fseek($handle, -$tail, SEEK_CUR);
                    $remaining += $tail;
                }
            }
        } finally {
            fclose($handle);
        }

        foreach ($out as $path => &$dateIdCounts) {
            ksort($dateIdCounts);
        }
        unset($dateIdCounts);

        $this->jsonize($outputPath, $out, $dates);
    }
    
    private function processChunk(string $chunk, array $dateIds, array &$out): void
    {
        $pos = 0;
        $chunkLen = strlen($chunk);
        
        while ($pos < $chunkLen) {
            $nlPos = strpos($chunk, "\n", $pos);
            if ($nlPos === false) {
                $line = substr($chunk, $pos);
                if ($line !== '') {
                    $this->processLine($line, $dateIds, $out);
                }
                break;
            }
            
            $line = substr($chunk, $pos, $nlPos - $pos);
            if ($line !== '') {
                $this->processLine($line, $dateIds, $out);
            }
            $pos = $nlPos + 1;
        }
    }
    
    private function processLine(string $line, array $dateIds, array &$out): void
    {
        // skipping https://stitcher.io/, it's all the same domain anyway
        // also skipping 15ch of datetime
        $line = substr($line, 19, -15);
        
        $commaPos = strpos($line, ',');
        if ($commaPos === false) return;
        
        $path = substr($line, 0, $commaPos);
        $dateFull = substr($line, $commaPos + 1, 10);

        $date = substr($dateFull, 2);
        
        $dateKey = $dateIds[$date] ?? $dateFull;
        
        if (!isset($out[$path])) {
            $out[$path] = [];
        }
        $out[$path][$dateKey] = ($out[$path][$dateKey] ?? 0) + 1;
    }

    private function jsonize($filename, &$out, array $dates) {
        $file = fopen($filename, 'wb');
        stream_set_write_buffer($file, self::WRITE_BUF);
        fwrite($file, '{');

        $isFirst = true;
        foreach ($out as $k => $ds) {
            $buf = $isFirst ? "\n    \"" : ",\n    \"";
            $buf .= str_replace('/', '\\/', $k)."\": {\n";
            $firstDate = true;
            foreach ($ds as $dateId => $v) {
                $dateStr = is_int($dateId) ? '20' . $dates[$dateId] : $dateId;
                $buf .= ($firstDate ? '' : ",\n") . "        \"$dateStr\": $v";
                $firstDate = false;
            }
            $buf .= "\n    }";
            fwrite($file, $buf);
            $isFirst = false;
        }

        fwrite($file, "\n}");
        fclose($file);
    }
}