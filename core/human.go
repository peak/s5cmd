package core

import (
	"fmt"
	"strconv"
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
