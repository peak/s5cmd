package complete

import (
	"os"
	"path/filepath"
)

// relativePath changes a file name to a relative name
func relativePath(file string) string {
	// get wording directory for relative name
	workDir, err := os.Getwd()
	if err != nil {
		return file
	}

	abs, err := filepath.Abs(file)
	if err != nil {
		return file
	}
	rel, err := filepath.Rel(workDir, abs)
	if err != nil {
		return file
	}
	if rel != "." {
		rel = "./" + rel
	}
	if info, err := os.Stat(rel); err == nil && info.IsDir() {
		rel += "/"
	}
	return rel
}
