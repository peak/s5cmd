package command

import (
	"fmt"
	"net/url"

	"github.com/peak/s5cmd/storage"
	"github.com/urfave/cli/v2"
)

const versioningNotSupportedWarning string = "versioning related features are not supported with the given endpoint %q"

func checkVersioningWithGoogleEndpoint(ctx *cli.Context) error {
	endpoint := ctx.String("endpoint-url")
	if endpoint == "" {
		return nil
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}

	if storage.IsGoogleEndpoint(*u) && (ctx.Bool("all-versions") || ctx.String("version-id") != "") {
		return fmt.Errorf(versioningNotSupportedWarning, endpoint)
	}

	return nil
}
