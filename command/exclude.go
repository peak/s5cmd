package command

import (
	"regexp"
	"strings"
)

func wildCardToRegexp(pattern string) string {
	patternRegex := regexp.QuoteMeta(pattern)
	patternRegex = strings.Replace(patternRegex, "\\?", ".", -1)
	patternRegex = strings.Replace(patternRegex, "\\*", ".*", -1)
	return patternRegex
}

func regexMatch(pattern string, value string) bool {
	result, _ := regexp.MatchString(pattern, value)
	return result
}

// createExcludesFromWildcard creates regex strings from wildcard.
func createExcludesFromWildcard(inputExcludes []string) []string {
	result := make([]string, 0)
	for _, input := range inputExcludes {
		result = append(result, wildCardToRegexp(input))
	}
	return result
}

// isURLExcluded checks whether given urlPath matches any of the exclude patterns.
func isURLExcluded(excludePatterns []string, urlPath string) bool {
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
