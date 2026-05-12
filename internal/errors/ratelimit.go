package errors

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// MaxCooldownDuration is the maximum cooldown time we'll apply to a key.
// Even if the upstream says "try again in 12h", we cap it to avoid keys being unusable for too long.
const MaxCooldownDuration = 5 * time.Minute

// MinCooldownDuration is the minimum cooldown time to apply.
const MinCooldownDuration = 10 * time.Second

// retryAfterRegex matches "Please try again in XhYmZ.ZZZs" patterns
var retryAfterRegex = regexp.MustCompile(`try again in\s+((\d+)h)?((\d+)m)?((\d+(?:\.\d+)?)s)?`)

// IsRateLimitError checks if the error message indicates a rate limit (429) error.
func IsRateLimitError(errorMsg string) bool {
	if errorMsg == "" {
		return false
	}
	lower := strings.ToLower(errorMsg)
	return strings.Contains(lower, "rate limit reached") ||
		strings.Contains(lower, "tokens per min") ||
		strings.Contains(lower, "requests per min") ||
		strings.Contains(lower, "rate_limit_exceeded")
}

// ParseRetryAfter extracts the retry-after duration from an error message.
// Returns 0 if no duration could be parsed.
func ParseRetryAfter(errorMsg string) time.Duration {
	matches := retryAfterRegex.FindStringSubmatch(errorMsg)
	if matches == nil {
		return 0
	}

	var total time.Duration

	// hours (group 2)
	if matches[2] != "" {
		h, _ := strconv.Atoi(matches[2])
		total += time.Duration(h) * time.Hour
	}

	// minutes (group 4)
	if matches[4] != "" {
		m, _ := strconv.Atoi(matches[4])
		total += time.Duration(m) * time.Minute
	}

	// seconds (group 6)
	if matches[6] != "" {
		s, _ := strconv.ParseFloat(matches[6], 64)
		total += time.Duration(s * float64(time.Second))
	}

	return total
}

// GetCooldownDuration returns the actual cooldown duration to apply, capped by MaxCooldownDuration.
func GetCooldownDuration(errorMsg string) time.Duration {
	parsed := ParseRetryAfter(errorMsg)
	if parsed <= 0 {
		return MinCooldownDuration
	}
	if parsed > MaxCooldownDuration {
		return MaxCooldownDuration
	}
	if parsed < MinCooldownDuration {
		return MinCooldownDuration
	}
	return parsed
}
