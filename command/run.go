package command

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/kballard/go-shellquote"
	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/v2/parallel"
)

var runHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [file]

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Run the commands declared in "commands.txt" file in parallel
		 > s5cmd {{.HelpName}} commands.txt

	2. Read commands from standard input and execute in parallel.
		 > cat commands.txt | s5cmd {{.HelpName}}
`

func NewRunCommand() *cli.Command {
	return &cli.Command{
		Name:               "run",
		HelpName:           "run",
		Usage:              "run commands in batch",
		CustomHelpTemplate: runHelpTemplate,
		Before: func(c *cli.Context) error {
			err := validateRunCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) error {
			reader := os.Stdin
			if c.Args().Len() == 1 {
				f, err := os.Open(c.Args().First())
				if err != nil {
					printError(commandFromContext(c), c.Command.Name, err)
					return err
				}
				defer f.Close()

				reader = f
			}

			return NewRun(c, reader).Run(c.Context)
		},
	}
}

type Run struct {
	c      *cli.Context
	reader io.Reader

	// flags
	numWorkers int
}

func NewRun(c *cli.Context, r io.Reader) Run {
	return Run{
		c:          c,
		reader:     r,
		numWorkers: c.Int("numworkers"),
	}
}

func (r Run) Run(ctx context.Context) error {
	pm := parallel.New(r.numWorkers)
	defer pm.Close()

	waiter := parallel.NewWaiter()

	var errDoneCh = make(chan struct{})
	var merrorWaiter error
	go func() {
		defer close(errDoneCh)
		for err := range waiter.Err() {
			merrorWaiter = multierror.Append(merrorWaiter, err)
		}
	}()

	reader := NewReader(ctx, r.reader)

	lineno := -1
	for line := range reader.Read() {
		lineno++

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// skip comment lines
		if strings.HasPrefix(line, "#") {
			continue
		}

		fields, err := shellquote.Split(line)
		if err != nil {
			return err
		}

		if len(fields) == 0 {
			continue
		}

		if fields[0] == "run" {
			err := fmt.Errorf("%q command (line: %v) is not permitted in run-mode", "run", lineno)
			printError(commandFromContext(r.c), r.c.Command.Name, err)
			continue
		}

		fn := func() error {
			subcmd := fields[0]

			cmd := AppCommand(subcmd)
			if cmd == nil {
				err := fmt.Errorf("%q command (line: %v) not found", subcmd, lineno)
				printError(commandFromContext(r.c), r.c.Command.Name, err)
				return nil
			}

			flagset := flag.NewFlagSet(subcmd, flag.ExitOnError)
			if err := flagset.Parse(fields); err != nil {
				printError(commandFromContext(r.c), r.c.Command.Name, err)
				return nil
			}

			ctx := cli.NewContext(app, flagset, r.c)
			return cmd.Run(ctx)
		}

		pm.Run(fn, waiter)
	}

	waiter.Wait()
	<-errDoneCh

	if reader.Err() != nil {
		printError(commandFromContext(r.c), r.c.Command.Name, reader.Err())
	}

	return multierror.Append(merrorWaiter, reader.Err()).ErrorOrNil()
}

// Reader is a cancelable reader.
type Reader struct {
	*bufio.Reader
	err    error
	linech chan string
	ctx    context.Context
}

// NewReader creates a new reader with cancellation.
func NewReader(ctx context.Context, r io.Reader) *Reader {
	reader := &Reader{
		ctx:    ctx,
		Reader: bufio.NewReader(r),
		linech: make(chan string),
	}

	go reader.read()
	return reader
}

// read reads lines from the underlying reader.
func (r *Reader) read() {
	defer close(r.linech)

	for {
		select {
		case <-r.ctx.Done():
			r.err = r.ctx.Err()
			return
		default:
			// If ReadString encounters an error before finding a delimiter,
			// it returns the data read before the error and the error itself (often io.EOF).
			line, err := r.ReadString('\n')
			if line != "" {
				r.linech <- line
			}
			if err != nil {
				if err == io.EOF {
					if errors.Is(r.ctx.Err(), context.Canceled) {
						r.err = r.ctx.Err()
					}
					return
				}
				r.err = multierror.Append(r.err, err)
			}
		}
	}
}

// Read returns read-only channel to consume lines.
func (r *Reader) Read() <-chan string {
	return r.linech
}

// Err returns encountered errors, if any.
func (r *Reader) Err() error {
	return r.err
}

func validateRunCommand(c *cli.Context) error {
	if c.Args().Len() > 1 {
		return fmt.Errorf("expected only 1 file")
	}
	return nil
}
