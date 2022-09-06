package command

import (
	"fmt"
	urlpkg "net/url"

	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
	"github.com/urfave/cli/v2"
)

const (
	versioningNotSupportedWarning = "versioning related features are not supported with the given endpoint %q"
	allVersionsFlagName           = "all-versions"
	versionIDFlagName             = "version-id"
)

// checkVersinoningURLRemote checks if the versioning related flags are used with
// local objects. Because the versioning is only supported with s3.
func checkVersinoningURLRemote(url *url.URL) error {
	if !url.IsRemote() && url.IsVersioned() {
		return fmt.Errorf("%q, and %q flags can only be used with remote objects", allVersionsFlagName, versionIDFlagName)
	}
	return nil
}

// checkVersioningFlagCompatibility checks if the incompatible versioning flags
// are used together. Because it is not allowed to refer to both "all versions" and
// a specific version of an object together.
func checkVersioningFlagCompatibility(ctx *cli.Context) error {
	if ctx.Bool(allVersionsFlagName) && ctx.String(versionIDFlagName) != "" {
		return fmt.Errorf("it is not allowed to combine %q and %q flags", allVersionsFlagName, versionIDFlagName)
	}
	return nil
}

// checkVersioningWithGoogleEndpoint checks if the versioning flags are used with
// the Google Endpoint. Because the s3 versioning operations are not compatible with
// GCS's versioning API.
func checkVersioningWithGoogleEndpoint(ctx *cli.Context) error {
	endpoint := ctx.String("endpoint-url")
	if endpoint == "" {
		return nil
	}

	u, err := urlpkg.Parse(endpoint)
	if err != nil {
		return err
	}

	if storage.IsGoogleEndpoint(*u) && (ctx.Bool(allVersionsFlagName) || ctx.String(versionIDFlagName) != "") {
		return fmt.Errorf(versioningNotSupportedWarning, endpoint)
	}

	return nil
}
