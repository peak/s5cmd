package version

import "strings"

var (
	Version   = "v0.0.0"
	GitCommit = "dev"
)

func GetHumanVersion() string {
	version := Version
	if !strings.HasPrefix(version, "v") {
		version = "v" + Version
	}

	return version + "-" + GitCommit
}
