package command

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/parallel"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

var selectHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] argument

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	01. Search for all JSON objects with the foo property set to 'bar' and spit them into stdout
		 > s5cmd {{.HelpName}} --compression gzip --query "SELECT * FROM S3Object s WHERE s.foo='bar'" "s3://bucket/*"
`

func NewSelectCommand() *cli.Command {
	cmd := &cli.Command{
		Name:     "select",
		HelpName: "select",
		Usage:    "run SQL queries on objects",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "query",
				Aliases: []string{"e"},
				Usage:   "SQL expression to use to select from the objects",
			},
			&cli.StringFlag{
				Name:  "compression",
				Usage: "input compression format",
				Value: "NONE",
			},
			&cli.GenericFlag{
				Name:  "format",
				Usage: "input data format (only JSON supported for the moment)",
				Value: &EnumValue{
					Enum:    []string{"JSON"},
					Default: "JSON",
				},
			},
			&cli.StringSliceFlag{
				Name:  "exclude",
				Usage: "exclude objects with given pattern",
			},
			&cli.BoolFlag{
				Name:  "force-glacier-transfer",
				Usage: "force transfer of glacier objects whether they are restored or not",
			},
			&cli.BoolFlag{
				Name:  "ignore-glacier-warnings",
				Usage: "turns off glacier warnings: ignore errors encountered during selecting objects",
			},
			&cli.BoolFlag{
				Name:  "raw",
				Usage: "disable the wildcard operations, useful with filenames that contains glob characters",
			},
			&cli.BoolFlag{
				Name:  "all-versions",
				Usage: "list all versions of object(s)",
			},
			&cli.StringFlag{
				Name:  "version-id",
				Usage: "use the specified version of the object",
			},
		},
		CustomHelpTemplate: selectHelpTemplate,
		Before: func(c *cli.Context) error {
			err := validateSelectCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			fullCommand := commandFromContext(c)

			src, err := url.New(c.Args().Get(0), url.WithVersion(c.String("version-id")),
				url.WithRaw(c.Bool("raw")), url.WithAllVersions(c.Bool("all-versions")))
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}
			return Select{
				src:         src,
				op:          c.Command.Name,
				fullCommand: fullCommand,
				// flags
				query:                 c.String("query"),
				compressionType:       c.String("compression"),
				exclude:               c.StringSlice("exclude"),
				forceGlacierTransfer:  c.Bool("force-glacier-transfer"),
				ignoreGlacierWarnings: c.Bool("ignore-glacier-warnings"),

				storageOpts: NewStorageOpts(c),
			}.Run(c.Context)
		},
	}

	cmd.BashComplete = getBashCompleteFn(cmd, true, false)
	return cmd
}

// Select holds select operation flags and states.
type Select struct {
	src         *url.URL
	op          string
	fullCommand string

	query                 string
	compressionType       string
	exclude               []string
	forceGlacierTransfer  bool
	ignoreGlacierWarnings bool

	// s3 options
	storageOpts storage.Options
}

// Run starts copying given source objects to destination.
func (s Select) Run(ctx context.Context) error {
	client, err := storage.NewRemoteClient(ctx, s.src, s.storageOpts)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	objch, err := expandSource(ctx, client, false, s.src)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	var (
		merrorWaiter  error
		merrorObjects error
	)

	waiter := parallel.NewWaiter()
	errDoneCh := make(chan bool)
	writeDoneCh := make(chan bool)
	resultCh := make(chan json.RawMessage, 128)

	go func() {
		defer close(errDoneCh)
		for err := range waiter.Err() {
			printError(s.fullCommand, s.op, err)
			merrorWaiter = multierror.Append(merrorWaiter, err)
		}
	}()

	go func() {
		defer close(writeDoneCh)
		var fatalError error
		for {
			record, ok := <-resultCh
			if !ok {
				break
			}
			if fatalError != nil {
				// Drain the channel.
				continue
			}
			if _, err := os.Stdout.Write(append(record, '\n')); err != nil {
				// Stop reading upstream. Notably useful for EPIPE.
				cancel()
				printError(s.fullCommand, s.op, err)
				fatalError = err
			}
		}
	}()

	excludePatterns, err := createExcludesFromWildcard(s.exclude)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	for object := range objch {
		if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			merrorObjects = multierror.Append(merrorObjects, err)
			printError(s.fullCommand, s.op, err)
			continue
		}

		if object.StorageClass.IsGlacier() && !s.forceGlacierTransfer {
			if !s.ignoreGlacierWarnings {
				err := fmt.Errorf("object '%v' is on Glacier storage", object)
				merrorObjects = multierror.Append(merrorObjects, err)
				printError(s.fullCommand, s.op, err)
			}
			continue
		}

		if isURLExcluded(excludePatterns, object.URL.Path, s.src.Prefix) {
			continue
		}

		task := s.prepareTask(ctx, client, object.URL, resultCh)
		parallel.Run(task, waiter)

	}

	waiter.Wait()
	close(resultCh)
	<-errDoneCh
	<-writeDoneCh

	return multierror.Append(merrorWaiter, merrorObjects).ErrorOrNil()
}

func (s Select) prepareTask(ctx context.Context, client *storage.S3, url *url.URL, resultCh chan<- json.RawMessage) func() error {
	return func() error {
		query := &storage.SelectQuery{
			ExpressionType:  "SQL",
			Expression:      s.query,
			CompressionType: s.compressionType,
		}

		return client.Select(ctx, url, query, resultCh)
	}
}

func validateSelectCommand(c *cli.Context) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("expected source argument")
	}

	if err := checkVersioningFlagCompatibility(c); err != nil {
		return err
	}

	if err := checkVersioningWithGoogleEndpoint(c); err != nil {
		return err
	}

	srcurl, err := url.New(c.Args().Get(0), url.WithVersion(c.String("version-id")),
		url.WithRaw(c.Bool("raw")), url.WithAllVersions(c.Bool("all-versions")))
	if err != nil {
		return err
	}

	if !srcurl.IsRemote() {
		return fmt.Errorf("source must be remote")
	}

	if !strings.EqualFold(c.String("format"), "JSON") {
		return fmt.Errorf("only json supported")
	}

	return nil
}
