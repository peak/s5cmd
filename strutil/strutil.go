package strutil

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var humanDivisors = [...]struct {
	suffix string
	div    int64
}{
	{"K", 1 << 10},
	{"M", 1 << 20},
	{"G", 1 << 30},
	{"T", 1 << 40},
}

// HumanizeBytes takes a byte-size and returns a human-readable string
func HumanizeBytes(b int64) string {
	var (
		suffix string
		div    int64
	)
	for _, f := range humanDivisors {
		if b > f.div {
			suffix = f.suffix
			div = f.div
		}
	}
	if suffix == "" {
		return strconv.FormatInt(b, 10)
	}

	return fmt.Sprintf("%.1f%s", float64(b)/float64(div), suffix)
}

// JSON is a helper function for creating JSON-encoded strings.
func JSON(v interface{}) string {
	bytes, _ := json.Marshal(v)
	return string(bytes)
}

func wildCardToRegexp(pattern string) string {
	patternRegex := regexp.QuoteMeta(pattern)
	patternRegex = strings.Replace(patternRegex, "\\?", ".", -1)
	patternRegex = strings.Replace(patternRegex, "\\*", ".*", -1)
	return patternRegex
}

func regexMatch(pattern string, value string) bool {
	result, _ := regexp.MatchString(wildCardToRegexp(pattern), value)
	return result
}

func IsURLExcluded(excludePatterns []string, urlPath string) bool {

	if len(excludePatterns) == 0 {
		return false
	}

	for _, excludePattern := range excludePatterns {
		if excludePattern != "" && regexMatch(excludePattern, urlPath) {
			return true
		}
	}
	return false
}
