package command

import (
	"context"
	"fmt"

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
	
	3. Print a remote object's metadata with human-readable sizes
		 > s5cmd {{.HelpName}} --humanize s3://bucket/prefix/object
	
	4. Print a remote object's metadata with ETag
		 > s5cmd {{.HelpName}} --etag s3://bucket/prefix/object
	
	5. Print a remote object's fullpath
		 > s5cmd {{.HelpName}} --show-fullpath s3://bucket/prefix/object
	
	6. Print a remote object's metadata with version ID
		 > s5cmd {{.HelpName}} --version-id VERSION_ID s3://bucket/prefix/object
	
	7. Print a remote object's metadata with raw input
		 > s5cmd {{.HelpName}} --raw s3://bucket/prefix/object

`

func NewHeadCommand() *cli.Command {
	cmd := &cli.Command{
		Name:     "head",
		HelpName: "head",
		Usage:    "print remote object metadata",

		CustomHelpTemplate: headHelpTemplate,

		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "etag",
				Aliases: []string{"e"},
				Usage:   "show entity tag (ETag) in the output",
			},
			&cli.BoolFlag{
				Name:    "humanize",
				Aliases: []string{"H"},
				Usage:   "human-readable output for object sizes",
			},
			&cli.BoolFlag{
				Name:    "storage-class",
				Aliases: []string{"s"},
				Usage:   "display full name of the object class",
				Value:   true,
			},
			&cli.BoolFlag{
				Name:  "show-fullpath",
				Usage: "shows only the fullpath names of the object(s)",
			},
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
				// flags
				showEtag:         c.Bool("etag"),
				humanize:         c.Bool("humanize"),
				showStorageClass: c.Bool("storage-class"),
				showFullPath:     c.Bool("show-fullpath"),

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

	showEtag         bool
	humanize         bool
	showStorageClass bool
	showFullPath     bool

	storageOpts storage.Options
}

func (h Head) Run(ctx context.Context) error {
	h.src.SetRelative(h.src)
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
			Name: h.src.String(),
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
		Object:           object,
		showEtag:         h.showEtag,
		showHumanized:    h.humanize,
		showStorageClass: h.showStorageClass,
		showFullPath:     h.showFullPath,
		Metadata:         metadata,
	}

	log.Info(msg)

	return nil
}

type HeadObjectMessage struct {
	Object *storage.Object `json:"object"`

	showEtag         bool
	showHumanized    bool
	showStorageClass bool
	showFullPath     bool
	Metadata         map[string]string `json:"metadata"`
}

func (m HeadObjectMessage) String() string {
	if m.showFullPath {
		return m.Object.URL.String()
	}
	var etag string
	// date and storage fields
	listFormat := "%19s %2s"

	// align etag
	if m.showEtag {
		etag = m.Object.Etag
		listFormat = listFormat + " %-38s"
	} else {
		listFormat = listFormat + " %-1s"
	}

	// format file size
	listFormat = listFormat + " %12s "
	// format key and version ID
	if m.Object.URL.VersionID != "" {
		listFormat = listFormat + " %-50s %s"
	} else {
		listFormat = listFormat + " %s%s"
	}

	var s string
	if m.Object.Type.IsDir() {
		s = fmt.Sprintf(
			listFormat,
			"",
			"",
			"",
			"DIR",
			m.Object.URL.Relative(),
			"",
		)
		return s
	}

	stclass := ""
	if m.showStorageClass {
		stclass = fmt.Sprintf("%v", m.Object.StorageClass)
	}

	var path string
	if m.showFullPath {
		path = m.Object.URL.String()
	} else {
		path = m.Object.URL.Relative()
	}

	s = fmt.Sprintf(
		listFormat,
		m.Object.ModTime.Format(dateFormat),
		stclass,
		etag,
		m.humanize(),
		path,
		m.Object.URL.VersionID,
	)

	return s
}

func (m HeadObjectMessage) JSON() string {
	j := struct {
		storage.Object
		Metadata map[string]string `json:"metadata"`
	}{
		Object:   *m.Object,
		Metadata: m.Metadata,
	}

	return strutil.JSON(j)
}

func (m HeadObjectMessage) humanize() string {
	var size string
	if m.showHumanized {
		size = strutil.HumanizeBytes(m.Object.Size)
	} else {
		size = fmt.Sprintf("%d", m.Object.Size)
	}
	return size
}

type HeadBucketMessage struct {
	Name string `json:"name"`
}

func (m HeadBucketMessage) String() string {
	return fmt.Sprintf(m.Name)
}

func (m HeadBucketMessage) JSON() string {
	return strutil.JSON(m)
}

func validateHeadCommand(c *cli.Context) error {
	if c.Args().Len() > 1 {
		return fmt.Errorf("expected only 1 argument")
	}

	srcurl, err := url.New(c.Args().Get(0), url.WithVersion(c.String("version-id")),
		url.WithRaw(c.Bool("raw")))
	if err != nil {
		return err
	}

	if srcurl.IsWildcard() {
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
