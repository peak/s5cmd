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

// CapitalizeFirstRune converts first rune to uppercase, and converts rest of
// the string to lower case.
func CapitalizeFirstRune(str string) string {
	if str == "" {
		return str
	}
	runes := []rune(str)
	first, rest := runes[0], runes[1:]
	return strings.ToUpper(string(first)) + strings.ToLower(string(rest))
}

// AddNewLineFlag adds a flag that allows . to match new line character "\n".
// It assumes that the pattern does not have any flags.
func AddNewLineFlag(pattern string) string {
	return "(?s)" + pattern
}

// WildCardToRegexp converts a wildcarded expresiion to equivalent regular expression
func WildCardToRegexp(pattern string) string {
	patternRegex := regexp.QuoteMeta(pattern)
	patternRegex = strings.Replace(patternRegex, "\\?", ".", -1)
	return strings.Replace(patternRegex, "\\*", ".*", -1)
}

// MatchFromStartToEnd enforces that the regex will match the full string
func MatchFromStartToEnd(pattern string) string {
	return "^" + pattern + "$"
}
