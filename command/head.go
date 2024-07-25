package command

import (
	"context"
	"fmt"
	"time"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
	"github.com/peak/s5cmd/v2/strutil"
	"github.com/urfave/cli/v2"
)

var headHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] source

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Print a remote object's metadata
		 > s5cmd {{.HelpName}} s3://bucket/prefix/object

	2. Check if a remote bucket exists
		 > s5cmd {{.HelpName}} s3://bucket

	3. Print a remote object's metadata with version ID
		 > s5cmd {{.HelpName}} --version-id VERSION_ID s3://bucket/prefix/object

	4. Print a remote object's metadata with raw input
		 > s5cmd {{.HelpName}} --raw 's3://bucket/prefix/file*.txt'
`

func NewHeadCommand() *cli.Command {
	cmd := &cli.Command{
		Name:     "head",
		HelpName: "head",
		Usage:    "print remote object metadata",

		CustomHelpTemplate: headHelpTemplate,

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "version-id",
				Usage: "use the specified version of an object",
			},
			&cli.BoolFlag{
				Name:  "raw",
				Usage: "disable the wildcard operations, useful with filenames that contains glob characters",
			},
		},

		Before: func(c *cli.Context) error {
			err := validateHeadCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			op := c.Command.Name
			fullCommand := commandFromContext(c)
			src, err := url.New(c.Args().Get(0), url.WithVersion(c.String("version-id")),
				url.WithRaw(c.Bool("raw")))
			if err != nil {
				printError(fullCommand, op, err)
				return err
			}

			return Head{
				src:         src,
				op:          op,
				fullCommand: fullCommand,
				storageOpts: NewStorageOpts(c),
			}.Run(c.Context)
		},
	}
	cmd.BashComplete = getBashCompleteFn(cmd, true, false)
	return cmd
}

type Head struct {
	src         *url.URL
	op          string
	fullCommand string
	storageOpts storage.Options
}

func (h Head) Run(ctx context.Context) error {
	client, err := storage.NewRemoteClient(ctx, h.src, h.storageOpts)
	if err != nil {
		printError(h.fullCommand, h.op, err)
		return err
	}

	if h.src.IsBucket() {
		err := client.HeadBucket(ctx, h.src)
		if err != nil {
			printError(h.fullCommand, h.op, err)
			return err
		}

		msg := HeadBucketMessage{
			Bucket: h.src.String(),
		}

		log.Info(msg)

		return nil
	}

	object, metadata, err := client.HeadObject(ctx, h.src)
	if err != nil {
		printError(h.fullCommand, h.op, err)
		return err
	}

	msg := HeadObjectMessage{
		Key:                  object.URL.String(),
		ContentType:          metadata.ContentType,
		ServerSideEncryption: metadata.EncryptionMethod,
		LastModified:         object.ModTime,
		ContentLength:        object.Size,
		StorageClass:         string(object.StorageClass),
		VersionID:            object.VersionID,
		ETag:                 object.Etag,
		Metadata:             metadata.UserDefined,
	}

	log.Info(msg)

	return nil
}

type HeadObjectMessage struct {
	Key                  string            `json:"key,omitempty"`
	ContentType          string            `json:"content_type,omitempty"`
	ServerSideEncryption string            `json:"server_side_encryption,omitempty"`
	LastModified         *time.Time        `json:"last_modified,omitempty"`
	ContentLength        int64             `json:"size,omitempty"`
	StorageClass         string            `json:"storage_class,omitempty"`
	VersionID            string            `json:"version_id,omitempty"`
	ETag                 string            `json:"etag,omitempty"`
	Metadata             map[string]string `json:"metadata"`
}

func (m HeadObjectMessage) String() string {
	return m.JSON()
}

func (m HeadObjectMessage) JSON() string {
	return strutil.JSON(m)
}

type HeadBucketMessage struct {
	Bucket string `json:"bucket"`
}

func (m HeadBucketMessage) String() string {
	return m.JSON()
}

func (m HeadBucketMessage) JSON() string {
	return strutil.JSON(m)
}

func validateHeadCommand(c *cli.Context) error {
	if c.Args().Len() > 1 {
		return fmt.Errorf("object or bucket name is required")
	}

	srcurl, err := url.New(c.Args().Get(0), url.WithVersion(c.String("version-id")),
		url.WithRaw(c.Bool("raw")))
	if err != nil {
		return err
	}

	if srcurl.IsPrefix() {
		return fmt.Errorf("target have to be a object or a bucket")
	}

	if !srcurl.IsRemote() {
		return fmt.Errorf("target should be remote object or bucket")
	}

	if srcurl.IsWildcard() && !srcurl.IsRaw() {
		return fmt.Errorf("remote source %q can not contain glob characters", srcurl)
	}

	if err := checkVersinoningURLRemote(srcurl); err != nil {
		return err
	}

	if err := checkVersioningWithGoogleEndpoint(c); err != nil {
		return err
	}

	return nil
}
