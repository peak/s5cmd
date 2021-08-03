package command

import (
	"regexp"
	"strings"
)

func wildCardToRegexp(pattern string) string {
	patternRegex := regexp.QuoteMeta(pattern)
	patternRegex = strings.Replace(patternRegex, "\\?", ".", -1)
	patternRegex = strings.Replace(patternRegex, "\\*", ".*", -1)
	patternRegex = patternRegex + "$"
	return patternRegex
}

// createExcludesFromWildcard creates regex strings from wildcard.
func createExcludesFromWildcard(inputExcludes []string) []*regexp.Regexp {
	result := make([]*regexp.Regexp, 0)
	for _, input := range inputExcludes {
		if input != "" {
			regexVersion := wildCardToRegexp(input)
			regexpCompiled, err := regexp.Compile(regexVersion)
			if err != nil {
				continue
			}
			result = append(result, regexpCompiled)
		}
	}
	return result
}

// isURLExcluded checks whether given urlPath matches any of the exclude patterns.
func isURLExcluded(excludePatterns []*regexp.Regexp, urlPath string) bool {
	if len(excludePatterns) == 0 {
		return false
	}

	for _, excludePattern := range excludePatterns {
		if excludePattern.MatchString(urlPath) {
			return true
		}
	}
	return false
}
