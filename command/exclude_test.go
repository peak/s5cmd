package command

import (
	"reflect"
	"testing"

	"github.com/peak/s5cmd/storage/url"
)

func Test_createExcludeUrlsSingleRemoteURL(t *testing.T) {
	sourceURLPath := "s3://bucket/*"
	sourceURL, err := url.New(sourceURLPath)
	if err != nil {
		t.Errorf("Error in URL Creation: %v\n", err)
	}

	excludes := []string{"*.txt", "main*"}
	stringURLs := []string{
		"s3://bucket/*.txt",
		"s3://bucket/main*",
	}
	wantedURLS, err := newURLs(false, stringURLs...)
	if err != nil {
		t.Errorf("Error in URL Creation: %v\n", err)
	}

	excludeURLs, err := createExcludeUrls(excludes, sourceURL)
	if err != nil {
		t.Errorf("Error in Exclude URL Creation: %v\n", err)
	}

	for i := range excludeURLs {
		if !reflect.DeepEqual(excludeURLs[i], wantedURLS[i]) {
			t.Errorf("wanted: %#v, got %#v\n", wantedURLS[i], excludeURLs[i])
		}
	}
}

func Test_createExcludeUrlsMultipleRemoteURLs(t *testing.T) {
	sourceURLPaths := []string{
		"s3://bucket/*",
		"s3://dstbucket/abc/*",
	}
	sourceURLs, err := newURLs(false, sourceURLPaths...)
	if err != nil {
		t.Errorf("Error in URL Creation: %v\n", err)
	}

	excludes := []string{"*.txt", "main*"}
	stringURLs := []string{
		"s3://bucket/*.txt",
		"s3://bucket/main*",
		"s3://dstbucket/abc/*.txt",
		"s3://dstbucket/abc/main*",
	}
	wantedURLS, err := newURLs(false, stringURLs...)
	if err != nil {
		t.Errorf("Error in URL Creation: %v\n", err)
	}

	excludeURLs, err := createExcludeUrls(excludes, sourceURLs...)
	if err != nil {
		t.Errorf("Error in Exclude URL Creation: %v\n", err)
	}

	for i := range excludeURLs {
		if !reflect.DeepEqual(excludeURLs[i], wantedURLS[i]) {
			t.Errorf("wanted: %#v, got %#v\n", wantedURLS[i], excludeURLs[i])
		}
	}
}

func Test_createExcludeUrlsSingleLocalSource(t *testing.T) {
	sourceURLPaths := []string{
		"storage",
	}
	sourceURLs, err := newURLs(false, sourceURLPaths...)
	if err != nil {
		t.Errorf("Error in URL Creation: %v\n", err)
	}

	excludes := []string{"*.go", "url*"}
	stringURLs := []string{
		"storage/*.go",
		"storage/url*",
	}
	wantedURLS, err := newURLs(false, stringURLs...)
	if err != nil {
		t.Errorf("Error in URL Creation: %v\n", err)
	}

	excludeURLs, err := createExcludeUrls(excludes, sourceURLs...)
	if err != nil {
		t.Errorf("Error in Exclude URL Creation: %v\n", err)
	}

	for i := range excludeURLs {
		if !reflect.DeepEqual(excludeURLs[i], wantedURLS[i]) {
			t.Errorf("wanted: %#v, got %#v\n", wantedURLS[i], excludeURLs[i])
		}
	}
}

func Test_createExcludeUrlsMultipleLocalSources(t *testing.T) {
	sourceURLPaths := []string{
		"e2e",
		"main.go",
	}
	sourceURLs, err := newURLs(false, sourceURLPaths...)
	if err != nil {
		t.Errorf("Error in URL Creation: %v\n", err)
	}

	excludes := []string{"*.go", "url*"}
	stringURLs := []string{
		"e2e/*.go",
		"e2e/main.go",
		"main.go*.go",
		"main.gourl*",
	}
	wantedURLS, err := newURLs(false, stringURLs...)
	if err != nil {
		t.Errorf("Error in URL Creation: %v\n", err)
	}

	excludeURLs, err := createExcludeUrls(excludes, sourceURLs...)
	if err != nil {
		t.Errorf("Error in Exclude URL Creation: %v\n", err)
	}

	for i := range excludeURLs {
		if !reflect.DeepEqual(excludeURLs[i], wantedURLS[i]) {
			t.Errorf("wanted: %#v, got %#v\n", wantedURLS[i], excludeURLs[i])
		}
	}
}
