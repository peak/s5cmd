package command

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
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
	01. Select the average price of the avocado and amount sold, set the output format as csv
		 > s5cmd select csv -use-header USE --query "SELECT s.avg_price, s.quantity FROM S3Object s WHERE s.item='avocado'" "s3://bucket/prices.csv"

	02. Query TSV files
		 > s5cmd select csv --delimiter=\t --use-header USE --query "SELECT s.avg_price, s.quantity FROM S3Object s WHERE s.item='avocado'" "s3://bucket/prices.tsv"

	03. Select a specific field in a JSON document
		 > s5cmd select json --structure document --query "SELECT s.tracking_id FROM s3object[*]['metadata']['.zattrs'] s" "s3://bucket/metadata.json"

	04. Query files that contain lines of JSON objects
		 > s5cmd select json --query "SELECT s.id FROM s3object s WHERE s.lineNumber = 1"
`

func beforeFunc(c *cli.Context) error {
	err := validateSelectCommand(c)
	if err != nil {
		printError(commandFromContext(c), c.Command.Name, err)
	}
	return err
}

func buildSelect(c *cli.Context, inputFormat string, inputStructure *string) (cmd *Select, err error) {
	defer stat.Collect(c.Command.FullName(), &err)()

	fullCommand := commandFromContext(c)

	src, err := url.New(
		c.Args().Get(0),
		url.WithVersion(c.String("version-id")),
		url.WithRaw(c.Bool("raw")),
		url.WithAllVersions(c.Bool("all-versions")),
	)

	if err != nil {
		printError(fullCommand, c.Command.Name, err)
		return nil, err
	}

	outputFormat := c.String("output-format")
	if c.String("output-format") == "" {
		outputFormat = inputFormat
	}

	cmd = &Select{
		src:         src,
		op:          c.Command.Name,
		fullCommand: fullCommand,
		// flags
		inputFormat:           inputFormat,
		outputFormat:          outputFormat,
		query:                 c.String("query"),
		compressionType:       c.String("compression"),
		exclude:               c.StringSlice("exclude"),
		forceGlacierTransfer:  c.Bool("force-glacier-transfer"),
		ignoreGlacierWarnings: c.Bool("ignore-glacier-warnings"),

		storageOpts: NewStorageOpts(c),
	}

	// parquet files don't have an input structure
	if inputStructure != nil {
		cmd.inputStructure = *inputStructure
	}
	return cmd, nil
}

func NewSelectCommand() *cli.Command {
	sharedFlags := []cli.Flag{
		&cli.StringFlag{
			Name:    "query",
			Aliases: []string{"e"},
			Usage:   "SQL expression to use to select from the objects",
		},
		&cli.StringFlag{
			Name:  "output-format",
			Usage: "output format of the result (options: json, csv)",
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
	}

	cmd := &cli.Command{
		Name:     "select",
		HelpName: "select",
		Usage:    "run SQL queries on objects",
		Subcommands: []*cli.Command{
			{
				Name:  "csv",
				Usage: "run queries on csv files",
				Flags: append([]cli.Flag{
					&cli.StringFlag{
						Name:  "delimiter for the csv file",
						Usage: "delimiter of the csv file.",
						Value: ",",
					},
					&cli.StringFlag{
						Name:  "use-header",
						Usage: "use header of the csv file. (options for AWS: IGNORE, NONE, USE)",
						Value: "NONE",
					},
					&cli.StringFlag{
						Name:  "compression",
						Usage: "input compression format (options for AWS: GZIP or BZIP2)",
					},
				}, sharedFlags...),
				CustomHelpTemplate: selectHelpTemplate,
				Before:             beforeFunc,
				Action: func(c *cli.Context) (err error) {
					delimiter := c.String("delimiter")
					// We are doing this because the delimiters that are special characters
					// are automatically escaped by go. We quote/unquote to capture the actual
					// delimiter. See: https://stackoverflow.com/questions/59952721/how-to-read-t-as-tab-in-golang
					quotedDelimiter, err := strconv.Unquote(`"` + delimiter + `"`)

					if err != nil {
						printError("select csv", c.Command.Name, err)
						return err
					}

					cmd, err := buildSelect(c, "csv", &quotedDelimiter)

					// Since this flag is only specific to csv
					// and we set a default value, we tend
					// to set it explicitly after building
					// the command rather than setting it
					// on the shared builder. We also
					// show the options, but we don't
					// constraint the options that can be
					// passed to this flag, since other
					// providers might support other options
					// that AWS does not support.
					cmd.fileHeaderInfo = c.String("use-header")
					if err != nil {
						printError(cmd.fullCommand, c.Command.Name, err)
						return err
					}
					return cmd.Run(c.Context)
				},
			},
			{
				Name:  "json",
				Usage: "run queries on json files",
				Flags: append([]cli.Flag{
					&cli.GenericFlag{
						Name:  "structure",
						Usage: "how objects are aligned in the json file, options:(lines, document)",
						Value: &EnumValue{
							Enum:    []string{"lines", "document"},
							Default: "lines",
							ConditionFunction: func(str, target string) bool {
								return strings.ToLower(target) == str
							},
						},
					},
					&cli.StringFlag{
						Name:  "compression",
						Usage: "input compression format (options for AWS: GZIP or BZIP2)",
					},
				}, sharedFlags...),
				CustomHelpTemplate: selectHelpTemplate,
				Before:             beforeFunc,
				Action: func(c *cli.Context) (err error) {
					structure := c.String("structure")
					cmd, err := buildSelect(c, "json", &structure)
					if err != nil {
						printError(cmd.fullCommand, c.Command.Name, err)
						return err
					}
					return cmd.Run(c.Context)
				},
			},
			{
				Name:               "parquet",
				Usage:              "run queries on parquet files",
				Flags:              sharedFlags,
				CustomHelpTemplate: selectHelpTemplate,
				Before:             beforeFunc,
				Action: func(c *cli.Context) (err error) {
					cmd, err := buildSelect(c, "parquet", nil)
					if err != nil {
						printError(cmd.fullCommand, c.Command.Name, err)
						return err
					}
					return cmd.Run(c.Context)
				},
			},
		},
		Flags: append([]cli.Flag{
			&cli.StringFlag{
				Name:  "compression",
				Usage: "input compression format (options for AWS: GZIP or BZIP2)",
			},
		}, sharedFlags...),
		Before: func(c *cli.Context) (err error) {
			if c.Args().Len() == 0 {
				err = fmt.Errorf("expected source argument")
				printError(commandFromContext(c), c.Command.Name, err)
				return err
			}
			return nil
		},
		Action: func(c *cli.Context) (err error) {
			// default fallback
			structure := "lines"
			cmd, err := buildSelect(c, "json", &structure)
			if err != nil {
				printError(cmd.fullCommand, c.Command.Name, err)
				return err
			}
			return cmd.Run(c.Context)
		},
		CustomHelpTemplate: selectHelpTemplate,
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
	inputFormat           string
	compressionType       string
	inputStructure        string
	fileHeaderInfo        string
	outputFormat          string
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
	errDoneCh := make(chan struct{})
	writeDoneCh := make(chan struct{})
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

	excludePatterns, err := createRegexFromWildcard(s.exclude)
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

		if isURLMatched(excludePatterns, object.URL.Path, s.src.Prefix) {
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
			ExpressionType:        "SQL",
			Expression:            s.query,
			InputFormat:           s.inputFormat,
			InputContentStructure: s.inputStructure,
			FileHeaderInfo:        s.fileHeaderInfo,
			OutputFormat:          s.outputFormat,
			CompressionType:       s.compressionType,
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

	srcurl, err := url.New(
		c.Args().Get(0),
		url.WithVersion(c.String("version-id")),
		url.WithRaw(c.Bool("raw")),
		url.WithAllVersions(c.Bool("all-versions")),
	)

	if err != nil {
		return err
	}

	if !srcurl.IsRemote() {
		return fmt.Errorf("source must be remote")
	}

	if c.String("query") == "" {
		return fmt.Errorf("query must be non-empty")
	}

	return nil
}
