package strutil

import (
	"encoding/json"
	"fmt"
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
