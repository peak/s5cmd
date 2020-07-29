package command

import (
	"github.com/peak/s5cmd/storage"
	"github.com/urfave/cli/v2"
)

// s3opts returns new S3Options object by extracting
// its fields from the provided context. Region is
// taken as (default) source-region.
func s3opts(c *cli.Context) storage.S3Options {
	region := c.String("source-region")
	if region == "" {
		region = c.String("default-source-region")
	}
	return storage.S3Options{
		MaxRetries:  c.Int("retry-count"),
		Endpoint:    c.String("endpoint-url"),
		NoVerifySSL: c.Bool("no-verify-ssl"),
		Region:      region,
	}
}

// dstS3opts returns new S3Options object by extracting
// its fields from the provided context. Region is
// taken as (default) region, i.e., destination region.
func dstS3opts(c *cli.Context) storage.S3Options {
	dstRegion := c.String("region")
	if dstRegion == "" {
		dstRegion = c.String("default-region")
	}
	return storage.S3Options{
		MaxRetries:  c.Int("retry-count"),
		Endpoint:    c.String("endpoint-url"),
		NoVerifySSL: c.Bool("no-verify-ssl"),
		Region:      dstRegion,
	}
}
