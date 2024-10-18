package command

import (
	"context"
	"fmt"

	urlpkg "net/url"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
	"github.com/peak/s5cmd/v2/strutil"
)

var sizeHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] argument

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Show disk usage of all objects in a bucket
		 > s5cmd {{.HelpName}} "s3://bucket/*"

	2. Show disk usage of all objects that match a wildcard, grouped by storage class
		 > s5cmd {{.HelpName}} --group "s3://bucket/prefix/obj*.gz"

	3. Show disk usage of all objects in a bucket but exclude the ones with py extension or starts with main
		 > s5cmd {{.HelpName}} --exclude "*.py" --exclude "main*" "s3://bucket/*"

	4. Show disk usage of all versions of an object in the bucket
		 > s5cmd {{.HelpName}} --all-versions s3://bucket/object

	5. Show disk usage of all versions of all objects that starts with a prefix in the bucket
		 > s5cmd {{.HelpName}} --all-versions "s3://bucket/prefix*"

	6. Show disk usage of all versions of all objects in the bucket
		 > s5cmd {{.HelpName}} --all-versions "s3://bucket/*"

	7. Show disk usage of a specific version of an object in the bucket
		 > s5cmd {{.HelpName}} --version-id VERSION_ID s3://bucket/object
`

func NewSizeCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "du",
		HelpName:           "du",
		Usage:              "show object size usage",
		CustomHelpTemplate: sizeHelpTemplate,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "group",
				Aliases: []string{"g"},
				Usage:   "group sizes by storage class",
			},
			&cli.BoolFlag{
				Name:    "humanize",
				Aliases: []string{"H"},
				Usage:   "human-readable output for object sizes",
			},
			&cli.StringSliceFlag{
				Name:  "exclude",
				Usage: "exclude objects with given pattern",
			},
			&cli.BoolFlag{
				Name:  "all-versions",
				Usage: "list all versions of object(s)",
			},
			&cli.StringFlag{
				Name:  "version-id",
				Usage: "use the specified version of an object",
			},
		},
		Before: func(c *cli.Context) error {
			err := validateDUCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			fullCommand := commandFromContext(c)

			srcurl, err := url.New(c.Args().First(),
				url.WithAllVersions(c.Bool("all-versions")),
				url.WithVersion(c.String("version-id")))
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}

			return Size{
				src:         srcurl,
				op:          c.Command.Name,
				fullCommand: fullCommand,
				// flags
				groupByClass: c.Bool("group"),
				humanize:     c.Bool("humanize"),
				exclude:      c.StringSlice("exclude"),

				storageOpts: NewStorageOpts(c),
			}.Run(c.Context)
		},
	}

	cmd.BashComplete = getBashCompleteFn(cmd, false, false)
	return cmd
}

// Size holds disk usage (du) operation flags and states.
type Size struct {
	src         *url.URL
	op          string
	fullCommand string

	// flags
	groupByClass bool
	humanize     bool
	exclude      []string

	storageOpts storage.Options
}

// Run calculates disk usage of given source.
func (sz Size) Run(ctx context.Context) error {
	client, err := storage.NewClient(ctx, sz.src, sz.storageOpts)
	if err != nil {
		printError(sz.fullCommand, sz.op, err)
		return err
	}

	storageTotal := map[string]sizeAndCount{}
	total := sizeAndCount{}

	var merror error

	excludePatterns, err := createRegexFromWildcard(sz.exclude)
	if err != nil {
		printError(sz.fullCommand, sz.op, err)
		return err
	}

	for object := range client.List(ctx, sz.src, false) {
		if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			merror = multierror.Append(merror, err)
			printError(sz.fullCommand, sz.op, err)
			continue
		}

		if isURLMatched(excludePatterns, object.URL.Path, sz.src.Prefix) {
			continue
		}

		storageClass := string(object.StorageClass)
		s := storageTotal[storageClass]
		s.addObject(object)
		storageTotal[storageClass] = s

		total.addObject(object)
	}

	if !sz.groupByClass {
		msg := SizeMessage{
			Source:        sz.src.String(),
			Count:         total.count,
			Size:          total.size,
			showHumanized: sz.humanize,
		}
		log.Info(msg)
		return nil
	}

	for k, v := range storageTotal {
		msg := SizeMessage{
			Source:        sz.src.String(),
			StorageClass:  k,
			Count:         v.count,
			Size:          v.size,
			showHumanized: sz.humanize,
		}
		log.Info(msg)
	}
	return merror
}

// SizeMessage is the structure for logging disk usage.
type SizeMessage struct {
	Source       string `json:"source"`
	StorageClass string `json:"storage_class,omitempty"`
	Count        int64  `json:"count"`
	Size         int64  `json:"size"`

	showHumanized bool
}

// humanize is a helper method to humanize bytes.
func (s SizeMessage) humanize() string {
	if s.showHumanized {
		return strutil.HumanizeBytes(s.Size)
	}
	return fmt.Sprintf("%d", s.Size)
}

// String returns the string representation of SizeMessage.
func (s SizeMessage) String() string {
	var storageCls string
	if s.StorageClass != "" {
		storageCls = fmt.Sprintf(" [%s]", s.StorageClass)
	}
	return fmt.Sprintf(
		"%s bytes in %d objects: %s%s",
		s.humanize(),
		s.Count,
		s.Source,
		storageCls,
	)
}

// JSON returns the JSON representation of SizeMessage.
func (s SizeMessage) JSON() string {
	return strutil.JSON(s)
}

type sizeAndCount struct {
	size  int64
	count int64
}

func (s *sizeAndCount) addObject(obj *storage.Object) {
	s.size += obj.Size
	s.count++
}

func validateDUCommand(c *cli.Context) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("expected only 1 argument")
	}

	if err := checkVersioningFlagCompatibility(c); err != nil {
		return err
	}

	srcurl, err := url.New(c.Args().First(),
		url.WithAllVersions(c.Bool("all-versions")))
	if err != nil {
		return err
	}

	if err := checkVersinoningURLRemote(srcurl); err != nil {
		return err
	}

	// the "all-versions" flag of du command works with GCS, because it does not
	// depend on the generation numbers.
	endpoint, err := urlpkg.Parse(c.String("endpoint-url"))
	if err == nil && c.String("version-id") != "" && storage.IsGoogleEndpoint(*endpoint) {
		return fmt.Errorf(versioningNotSupportedWarning, endpoint)
	}

	return nil
}
