package command

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/kballard/go-shellquote"
	"github.com/urfave/cli/v3"

	"github.com/peak/s5cmd/v2/parallel"
)

var runHelpTemplate = `Name:
	{{.FullName}} - {{.Usage}}

Usage:
	{{.FullName}} [file]

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Run the commands declared in "commands.txt" file in parallel
		 > {{.FullName}} commands.txt

	2. Read commands from standard input and execute in parallel.
		 > cat commands.txt | {{.FullName}}
`

func NewRunCommand() *cli.Command {
	return &cli.Command{
		Name:               "run",
		Usage:              "run commands in batch",
		CustomHelpTemplate: runHelpTemplate,
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			err := validateRunCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Name, err)
			}
			return ctx, err
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			reader := os.Stdin
			if c.Args().Len() == 1 {
				f, err := os.Open(c.Args().First())
				if err != nil {
					printError(commandFromContext(c), c.Name, err)
					return err
				}
				defer f.Close()

				reader = f
			}

			return NewRun(c, reader).Run(ctx)
		},
	}
}

type Run struct {
	c      *cli.Command
	reader io.Reader

	// flags
	numWorkers int
}

func NewRun(c *cli.Command, r io.Reader) Run {
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

	// A quoted argument may span physical lines, because an object key is
	// allowed to contain a newline and sync quotes such a key rather than
	// dropping it. Lines are therefore accumulated until they parse.
	var (
		pending       string
		pendingLineno int
	)

	for line := range reader.Read() {
		lineno++

		// ReadString keeps the delimiter it split on; drop just that, so
		// that a line break inside a quoted argument can be restored
		// exactly when lines are joined below.
		line = strings.TrimSuffix(line, "\n")

		if pending == "" {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			// skip comment lines
			if strings.HasPrefix(line, "#") {
				continue
			}

			pending, pendingLineno = line, lineno
		} else {
			// Inside an open quote the line break and any leading
			// whitespace are part of the argument, so keep them.
			pending += "\n" + line
		}

		fields, err := shellquote.Split(pending)
		if err != nil {
			if isUnterminatedQuote(err) {
				continue
			}
			return err
		}

		// error messages should point at the line the command started on
		lineno := pendingLineno
		pending = ""

		if len(fields) == 0 {
			continue
		}

		if fields[0] == "run" {
			err := fmt.Errorf("%q command (line: %v) is not permitted in run-mode", "run", lineno)
			printError(commandFromContext(r.c), r.c.Name, err)
			continue
		}

		fn := func() error {
			subcmd := fields[0]

			cmd := AppCommand(subcmd)
			if cmd == nil {
				err := fmt.Errorf("%q command (line: %v) not found", subcmd, lineno)
				printError(commandFromContext(r.c), r.c.Name, err)
				return nil
			}

			// v3's Command.Run takes the parent command from the
			// context, so this fresh command inherits the run
			// command's lineage and the global flags stay visible to
			// it -- which is what v2 needed a hand-built
			// cli.Context for.
			return cmd.Run(ctx, fields)
		}

		pm.Run(fn, waiter)
	}

	var merrorSyntax error
	if pending != "" {
		// input ended inside a quoted argument
		if _, err := shellquote.Split(pending); err != nil {
			merrorSyntax = fmt.Errorf("line: %v: %w", pendingLineno, err)
			printError(commandFromContext(r.c), r.c.Name, merrorSyntax)
		}
	}

	waiter.Wait()
	<-errDoneCh

	if reader.Err() != nil {
		printError(commandFromContext(r.c), r.c.Name, reader.Err())
	}

	return multierror.Append(merrorWaiter, reader.Err(), merrorSyntax).ErrorOrNil()
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

func validateRunCommand(c *cli.Command) error {
	if c.Args().Len() > 1 {
		return fmt.Errorf("expected only 1 file")
	}
	return nil
}

// isUnterminatedQuote reports whether err means the parser ran out of input
// mid-argument, so that reading a further line may complete it.
func isUnterminatedQuote(err error) bool {
	return errors.Is(err, shellquote.UnterminatedSingleQuoteError) ||
		errors.Is(err, shellquote.UnterminatedDoubleQuoteError) ||
		errors.Is(err, shellquote.UnterminatedEscapeError)
}
