<?php

namespace App;

final class Parser
{
    private const READ_CHUNK = 163_840;
    private const PREFIX_LEN = 25; // "https://stitcher.io/blog/"
    private const WRITE_BUF  = 1_048_576;

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);

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

        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $remaining = $fileSize;

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk = \fread($handle, $toRead);
            $chunkLen = \strlen($chunk);

            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                \fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = self::PREFIX_LEN;
            while ($p < $lastNl) {
                $sep = \strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $slug = \substr($chunk, $p, $sep - $p);
                $date = \substr($chunk, $sep + 3, 8);
                $dateKey = $dateIds[$date] ?? ('20' . $date);
                if (!isset($out[$slug])) $out[$slug] = [];
                $out[$slug][$dateKey] = ($out[$slug][$dateKey] ?? 0) + 1;
                $p = $sep + 52;
            }
        }

        \fclose($handle);

        foreach ($out as &$dateIdCounts) {
            \ksort($dateIdCounts);
        }
        unset($dateIdCounts);

        $this->jsonize($outputPath, $out, $dates);
    }

    private function jsonize($filename, &$out, array $dates) {
        $file = \fopen($filename, 'wb');
        \stream_set_write_buffer($file, self::WRITE_BUF);
        \fwrite($file, '{');

        $isFirst = true;
        foreach ($out as $k => $ds) {
            $buf = $isFirst ? "\n    " : ",\n    ";
            $isFirst = false;
            $buf .= "\"\\/blog\\/" . \str_replace('/', '\\/', $k) . "\": {\n";
            $firstDate = true;
            foreach ($ds as $dateId => $v) {
                $dateStr = \is_int($dateId) ? '20' . $dates[$dateId] : $dateId;
                $buf .= ($firstDate ? '' : ",\n") . "        \"$dateStr\": $v";
                $firstDate = false;
            }
            $buf .= "\n    }";
            \fwrite($file, $buf);
        }

        \fwrite($file, "\n}");
        \fclose($file);
    }
}
