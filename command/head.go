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

	2. Print specific version of a remote object's metadata
		 > s5cmd {{.HelpName}} --version-id VERSION_ID s3://bucket/prefix/object
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
				Name:  "all-versions",
				Usage: "list all versions of object(s)",
			},
			&cli.BoolFlag{
				Name:  "show-fullpath",
				Usage: "shows only the fullpath names of the object(s)",
			},
		},

		Before: func(c *cli.Context) error {
			err := validateHEADCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {

			//print the command name
			//fmt.Println(c.Command.FullName())

			defer stat.Collect(c.Command.FullName(), &err)()

			//fmt.Println("head command")

			op := c.Command.Name
			//fmt.Println("op: ", op)
			fullCommand := commandFromContext(c)

			//print the command name
			//fmt.Println("fullCommand: ", fullCommand)

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
				//flag
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
		bucket, err := client.HeadBucket(ctx, h.src)
		if err != nil {
			printError(h.fullCommand, h.op, err)
			return err
		}

		msg := HeadBucketMessage{
			Bucket: bucket,
		}

		log.Info(msg)

		return nil

	}

	object, metadata, err := client.HeadObject(ctx, h.src)

	//print the metadata all fields

	//fmt.Println("metadata: ", metadata)

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

func (l HeadObjectMessage) String() string {
	if l.showFullPath {
		return l.Object.URL.String()
	}
	var etag string
	// date and storage fiels
	var listFormat = "%19s %2s"

	// align etag
	if l.showEtag {
		etag = l.Object.Etag
		listFormat = listFormat + " %-38s"
	} else {
		listFormat = listFormat + " %-1s"
	}

	// format file size
	listFormat = listFormat + " %12s "
	// format key and version ID
	if l.Object.URL.VersionID != "" {
		listFormat = listFormat + " %-50s %s"
	} else {
		listFormat = listFormat + " %s%s"
	}

	var s string
	if l.Object.Type.IsDir() {
		s = fmt.Sprintf(
			listFormat,
			"",
			"",
			"",
			"DIR",
			l.Object.URL.Relative(),
			"",
		)
		return s
	}

	stclass := ""
	if l.showStorageClass {
		stclass = fmt.Sprintf("%v", l.Object.StorageClass)
	}

	var path string
	if l.showFullPath {
		path = l.Object.URL.String()
	} else {
		path = l.Object.URL.Relative()
	}

	s = fmt.Sprintf(
		listFormat,
		l.Object.ModTime.Format(dateFormat),
		stclass,
		etag,
		l.humanize(),
		path,
		l.Object.URL.VersionID,
	)

	return s
}

func (m HeadObjectMessage) JSON() string {
	return strutil.JSON(m.Object)
}

func (l HeadObjectMessage) humanize() string {
	var size string
	if l.showHumanized {
		size = strutil.HumanizeBytes(l.Object.Size)
	} else {
		size = fmt.Sprintf("%d", l.Object.Size)
	}
	return size
}

type HeadBucketMessage struct {
	Bucket *storage.Bucket `json:"bucket"`
}

func (l HeadBucketMessage) String() string {
	return l.Bucket.Name
}

func (m HeadBucketMessage) JSON() string {
	return strutil.JSON(m.Bucket)
}

func validateHEADCommand(c *cli.Context) error {
	if c.Args().Len() > 1 {
		return fmt.Errorf("expected only 1 argument")
	}

	srcurl, err := url.New(c.Args().First(),
		url.WithAllVersions(c.Bool("all-versions")))
	if err != nil {
		return err
	}

	if err := checkVersinoningURLRemote(srcurl); err != nil {
		return err
	}

	if err := checkVersioningWithGoogleEndpoint(c); err != nil {
		return err
	}

	return nil
}
