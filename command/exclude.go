package command

import (
	"path/filepath"
	"regexp"
	"strings"

	"github.com/peak/s5cmd/strutil"
)

// createExcludesFromWildcard creates regex strings from wildcard.
func createExcludesFromWildcard(inputExcludes []string) ([]*regexp.Regexp, error) {
	var result []*regexp.Regexp
	for _, input := range inputExcludes {
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

// isURLExcluded checks whether given urlPath matches any of the exclude patterns.
func isURLExcluded(excludePatterns []*regexp.Regexp, urlPath, sourcePrefix string) bool {
	if len(excludePatterns) == 0 {
		return false
	}
	if !strings.HasSuffix(sourcePrefix, "/") {
		sourcePrefix += "/"
	}
	sourcePrefix = filepath.ToSlash(sourcePrefix)
	for _, excludePattern := range excludePatterns {
		if excludePattern.MatchString(strings.TrimPrefix(urlPath, sourcePrefix)) {
			return true
		}
	}
	return false
}
