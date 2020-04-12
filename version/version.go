package version

import "strings"

var (
	// Version represents the git tag of a particular release.
	Version = "v0.0.0"

	// GitCommit represents git commit hash of a particular release.
	GitCommit = "dev"
)

// GetHumanVersion returns human readable version information.
func GetHumanVersion() string {
	version := Version
	if !strings.HasPrefix(version, "v") {
		version = "v" + Version
	}

	return version + "-" + GitCommit
}
