package command

import (
	"os"
	"path"

	"github.com/peak/s5cmd/storage/url"
)

// IsURLExcluded checks whether given urlPath matches any of the exclude patterns.
func isURLExcluded(srcurl *url.URL, excludeUrls []*url.URL) bool {
	if len(excludeUrls) == 0 {
		return false
	}

	for _, excludeUrl := range excludeUrls {
		if excludeUrl.Match(srcurl.Path) {
			return true
		}
	}
	return false
}

func createExcludeUrls(excludes []string, srcurls ...*url.URL) ([]*url.URL, error) {
	result := make([]*url.URL, 0)
	for _, srcurl := range srcurls {
		for _, exclude := range excludes {
			if exclude == "" {
				continue
			}
			sourcePrefix := srcurl.GetUntilPrefix()
			excludeStringUrl := sourcePrefix + exclude
			if !srcurl.IsRemote() {
				obj, err := os.Stat(srcurl.Absolute())
				if err != nil {
					continue
				}
				if obj.IsDir() {
					excludeStringUrl = path.Join(sourcePrefix, exclude)
				}
			}
			excludeUrl, err := url.New(excludeStringUrl)
			if err != nil {
				return nil, err
			}
			result = append(result, excludeUrl)
		}
	}
	return result, nil
}
