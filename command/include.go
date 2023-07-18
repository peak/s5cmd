package command

import (
	"path/filepath"
	"regexp"
	"strings"

	"github.com/peak/s5cmd/v2/strutil"
)

// createIncludesFromWildcard creates regex strings from wildcard.
func createIncludesFromWildcard(inputIncludes []string) ([]*regexp.Regexp, error) {
	var result []*regexp.Regexp
	for _, input := range inputIncludes {
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

// isURLIncluded checks whether given urlPath matches any of the include patterns.
func isURLIncluded(includePatterns []*regexp.Regexp, urlPath, sourcePrefix string) bool {
	if len(includePatterns) == 0 {
		return false
	}
	if !strings.HasSuffix(sourcePrefix, "/") {
		sourcePrefix += "/"
	}
	sourcePrefix = filepath.ToSlash(sourcePrefix)
	for _, includePattern := range includePatterns {
		if includePattern.MatchString(strings.TrimPrefix(urlPath, sourcePrefix)) {
			return true
		}
	}
	return false
}
