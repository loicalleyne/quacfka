package quackfka

import (
	"fmt"
	"strings"
	"time"
)

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%s %cB", formatLargeNumber(float64(bytes)/float64(div)), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	return d.Round(time.Millisecond).String()
}

func formatThroughput(t float64) string {
	return formatLargeNumber(t)
}

func formatLargeNumber(n float64) string {
	parts := strings.Split(fmt.Sprintf("%.2f", n), ".")
	intPart := parts[0]
	decPart := parts[1]

	var result []rune
	for i, c := range reverse(intPart) {
		if i > 0 && i%3 == 0 {
			result = append(result, '_')
		}
		result = append(result, c)
	}

	return fmt.Sprintf("%s.%s", string(reverse(string(result))), decPart)
}

func reverse(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func formatThroughputBytes(t float64) string {
	return fmt.Sprintf("%s/second", formatBytes(int64(t)))
}
