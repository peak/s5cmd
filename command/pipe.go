package command

import (
	"context"
	"fmt"
	"mime"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

var pipeHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] destination

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	01. Stream stdin output to an object
		 > echo "content" | gzip | s5cmd {{.HelpName}} s3://bucket/prefix/object.gz
`

func NewPipeCommandFlags() []cli.Flag {
	pipeFlags := []cli.Flag{
		&cli.StringFlag{
			Name:  "storage-class",
			Usage: "set storage class for target ('STANDARD','REDUCED_REDUNDANCY','GLACIER','STANDARD_IA','ONEZONE_IA','INTELLIGENT_TIERING','DEEP_ARCHIVE')",
		},
		&cli.IntFlag{
			Name:    "concurrency",
			Aliases: []string{"c"},
			Value:   defaultCopyConcurrency,
			Usage:   "number of concurrent parts transferred between host and remote server",
		},
		&cli.IntFlag{
			Name:    "part-size",
			Aliases: []string{"p"},
			Value:   defaultPartSize,
			Usage:   "size of each part transferred between host and remote server, in MiB",
		},
		&cli.StringFlag{
			Name:  "sse",
			Usage: "perform server side encryption of the data at its destination, e.g. aws:kms",
		},
		&cli.StringFlag{
			Name:  "sse-kms-key-id",
			Usage: "customer master key (CMK) id for SSE-KMS encryption; leave it out if server-side generated key is desired",
		},
		&cli.StringFlag{
			Name:  "acl",
			Usage: "set acl for target: defines granted accesses and their types on different accounts/groups, e.g. pipe --acl 'public-read'",
		},
		&cli.StringFlag{
			Name:  "cache-control",
			Usage: "set cache control for target: defines cache control header for object, e.g. pipe --cache-control 'public, max-age=345600'",
		},
		&cli.StringFlag{
			Name:  "expires",
			Usage: "set expires for target (uses RFC3339 format): defines expires header for object, e.g. pipe  --expires '2024-10-01T20:30:00Z'",
		},
		&cli.BoolFlag{
			Name:  "raw",
			Usage: "disable the wildcard operations, useful with filenames that contains glob characters",
		},
		&cli.StringFlag{
			Name:  "content-type",
			Usage: "set content type for target: defines content type header for object, e.g. --content-type text/plain",
		},
		&cli.StringFlag{
			Name:  "content-encoding",
			Usage: "set content encoding for target: defines content encoding header for object, e.g. --content-encoding gzip",
		},
		&cli.StringFlag{
			Name:  "content-disposition",
			Usage: "set content disposition for target: defines content disposition header for object, e.g. --content-disposition 'attachment; filename=\"filename.jpg\"'",
		},
		&cli.BoolFlag{
			Name:    "no-clobber",
			Aliases: []string{"n"},
			Usage:   "do not overwrite destination if already exists",
		},
	}
	return pipeFlags
}

func NewPipeCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "pipe",
		HelpName:           "pipe",
		Usage:              "strean to remote from stdin",
		Flags:              NewPipeCommandFlags(),
		CustomHelpTemplate: pipeHelpTemplate,
		Before: func(c *cli.Context) error {
			err := validatePipeCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			pipe, err := NewPipe(c, false)
			if err != nil {
				return err
			}
			return pipe.Run(c.Context)
		},
	}

	cmd.BashComplete = getBashCompleteFn(cmd, false, false)
	return cmd
}

// Pipe holds pipe operation flags and states.
type Pipe struct {
	dst         *url.URL
	op          string
	fullCommand string

	deleteSource bool

	// flags
	noClobber          bool
	storageClass       storage.StorageClass
	encryptionMethod   string
	encryptionKeyID    string
	acl                string
	cacheControl       string
	expires            string
	contentType        string
	contentEncoding    string
	contentDisposition string

	// s3 options
	concurrency int
	partSize    int64
	storageOpts storage.Options
}

// NewPipe creates Pipe from cli.Context.
func NewPipe(c *cli.Context, deleteSource bool) (*Pipe, error) {
	fullCommand := commandFromContext(c)

	dst, err := url.New(c.Args().Get(0), url.WithRaw(c.Bool("raw")))
	if err != nil {
		printError(fullCommand, c.Command.Name, err)
		return nil, err
	}

	return &Pipe{
		dst:          dst,
		op:           c.Command.Name,
		fullCommand:  fullCommand,
		deleteSource: deleteSource,
		// flags
		noClobber:          c.Bool("no-clobber"),
		storageClass:       storage.StorageClass(c.String("storage-class")),
		concurrency:        c.Int("concurrency"),
		partSize:           c.Int64("part-size") * megabytes,
		encryptionMethod:   c.String("sse"),
		encryptionKeyID:    c.String("sse-kms-key-id"),
		acl:                c.String("acl"),
		cacheControl:       c.String("cache-control"),
		expires:            c.String("expires"),
		contentType:        c.String("content-type"),
		contentEncoding:    c.String("content-encoding"),
		contentDisposition: c.String("content-disposition"),

		// s3 options
		storageOpts: NewStorageOpts(c),
	}, nil
}

// Run starts copying stdin output to destination.
func (c Pipe) Run(ctx context.Context) error {
	if c.dst.IsBucket() || c.dst.IsPrefix() {
		return fmt.Errorf("target %q must be an object", c.dst)
	}

	err := c.shouldOverride(ctx, c.dst)
	if err != nil {
		if errorpkg.IsWarning(err) {
			printDebug(c.op, err, nil, c.dst)
			return nil
		}
		return err
	}

	client, err := storage.NewRemoteClient(ctx, c.dst, c.storageOpts)
	if err != nil {
		return err
	}

	metadata := storage.NewMetadata().
		SetStorageClass(string(c.storageClass)).
		SetSSE(c.encryptionMethod).
		SetSSEKeyID(c.encryptionKeyID).
		SetACL(c.acl).
		SetCacheControl(c.cacheControl).
		SetExpires(c.expires)

	if c.contentType != "" {
		metadata.SetContentType(c.contentType)
	} else {
		metadata.SetContentType(guessContentTypeByExtension(c.dst))
	}

	if c.contentEncoding != "" {
		metadata.SetContentEncoding(c.contentEncoding)
	}

	if c.contentDisposition != "" {
		metadata.SetContentDisposition(c.contentDisposition)
	}

	err = client.Put(ctx, &stdin{file: os.Stdin}, c.dst, metadata, c.concurrency, c.partSize)
	if err != nil {
		return err
	}

	msg := log.InfoMessage{
		Operation:   c.op,
		Source:      nil,
		Destination: c.dst,
		Object: &storage.Object{
			StorageClass: c.storageClass,
		},
	}
	log.Info(msg)

	return nil
}

// shouldOverride function checks if the destination should be overridden if
// the destination object and given pipe flags conform to the
// override criteria.
func (c Pipe) shouldOverride(ctx context.Context, dsturl *url.URL) error {
	// if not asked to override, ignore.
	if !c.noClobber {
		return nil
	}

	client, err := storage.NewClient(ctx, dsturl, c.storageOpts)
	if err != nil {
		return err
	}

	obj, err := getObject(ctx, dsturl, client)
	if err != nil {
		return err
	}

	// if destination not exists, no conditions apply.
	if obj == nil {
		return nil
	}

	if c.noClobber {
		return errorpkg.ErrObjectExists
	}

	return nil
}

func validatePipeCommand(c *cli.Context) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("expected destination argument")
	}

	dst := c.Args().Get(0)

	dsturl, err := url.New(dst, url.WithRaw(c.Bool("raw")))
	if err != nil {
		return err
	}

	if !dsturl.IsRemote() {
		return fmt.Errorf("destination must be a bucket")
	}

	if dsturl.IsBucket() || dsturl.IsPrefix() {
		return fmt.Errorf("target %q must be an object", dsturl)
	}

	// wildcard destination can not be used with pipe
	if dsturl.IsWildcard() {
		return fmt.Errorf("target %q can not contain glob characters", dst)
	}

	return nil
}

func guessContentTypeByExtension(dsturl *url.URL) string {
	contentType := mime.TypeByExtension(filepath.Ext(dsturl.Absolute()))
	if contentType == "" {
		return "application/octet-stream"
	}
	return contentType
}

// stdin is an io.Reader adapter for os.File, enabling it to function solely as
// an io.Reader. The AWS SDK, which accepts an io.Reader for multipart uploads,
// will attempt to use io.Seek if the reader supports it. However, os.Stdin is
// a specific type of file that can not seekable.
type stdin struct {
	file *os.File
}

func (s *stdin) Read(p []byte) (n int, err error) {
	return s.file.Read(p)
}
