package command

import (
	"path/filepath"
	"regexp"
	"strings"

	"github.com/peak/s5cmd/v2/strutil"
)

// createRegexFromWildcard creates regex strings from wildcard.
func createRegexFromWildcard(wildcards []string) ([]*regexp.Regexp, error) {
	var result []*regexp.Regexp
	for _, input := range wildcards {
		if input != "" {
			regex := strutil.WildCardToRegexp(input)
			regex = strutil.MatchFromStartToEnd(regex)
			regex = strutil.AddNewLineFlag(regex)
			regexpCompiled, err := regexp.Compile(regex)
			if err != nil {
				return nil, err
			}
			result = append(result, regexpCompiled)
		}
	}
	return result, nil
}

func isURLMatched(regexPatterns []*regexp.Regexp, urlPath, sourcePrefix string) bool {
	if len(regexPatterns) == 0 {
		return false
	}
	if !strings.HasSuffix(sourcePrefix, "/") {
		sourcePrefix += "/"
	}
	sourcePrefix = filepath.ToSlash(sourcePrefix)
	for _, regexPattern := range regexPatterns {
		if regexPattern.MatchString(strings.TrimPrefix(urlPath, sourcePrefix)) {
			return true
		}
	}
	return false
}
