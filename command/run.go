package command

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/kballard/go-shellquote"
	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/parallel"
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

type Run struct {
	reader io.Reader
}

func NewRun(c *cli.Context, r io.Reader) Run {
	return Run{
		reader: r,
	}
}

func (r Run) Run(c *cli.Context) error {
	reader := r.reader
	pm := parallel.New(c.Int("numworkers"))
	defer pm.Close()

	waiter := parallel.NewWaiter()

	var errDoneCh = make(chan bool)
	var merrorWaiter error
	go func() {
		defer close(errDoneCh)
		for err := range waiter.Err() {
			merrorWaiter = multierror.Append(merrorWaiter, err)
		}
	}()

	scanner := NewScanner(c.Context, reader)

	lineno := -1
	for line := range scanner.Scan() {
		lineno++

		// support inline comments
		line = strings.Split(line, " #")[0]

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

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
			printError(givenCommand(c), c.Command.Name, err)
			continue
		}

		fn := func() error {
			subcmd := fields[0]

			cmd := AppCommand(subcmd)
			if cmd == nil {
				err := fmt.Errorf("%q command (line: %v) not found", subcmd, lineno)
				printError(givenCommand(c), c.Command.Name, err)
				return nil
			}

			flagset := flag.NewFlagSet(subcmd, flag.ExitOnError)
			if err := flagset.Parse(fields); err != nil {
				printError(givenCommand(c), c.Command.Name, err)
				return nil
			}

			ctx := cli.NewContext(app, flagset, c)
			return cmd.Run(ctx)
		}

		pm.Run(fn, waiter)
	}

	waiter.Wait()
	<-errDoneCh

	return multierror.Append(merrorWaiter, scanner.Err()).ErrorOrNil()
}

func NewRunCommand() *cli.Command {
	return &cli.Command{
		Name:               "run",
		HelpName:           "run",
		Usage:              "run commands in batch",
		CustomHelpTemplate: runHelpTemplate,
		Before: func(c *cli.Context) error {
			err := validateRunCommand(c)
			if err != nil {
				printError(givenCommand(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) error {
			reader := os.Stdin
			if c.Args().Len() == 1 {
				f, err := os.Open(c.Args().First())
				if err != nil {
					return err
				}
				defer f.Close()

				reader = f
			}

			return NewRun(c, reader).Run(c)
		},
	}
}

// Scanner is a cancelable scanner.
type Scanner struct {
	*bufio.Scanner
	err    error
	linech chan string
	ctx    context.Context
}

// NewScanner creates a new scanner with cancellation.
func NewScanner(ctx context.Context, r io.Reader) *Scanner {
	scanner := &Scanner{
		ctx:     ctx,
		Scanner: bufio.NewScanner(r),
		linech:  make(chan string),
	}

	go scanner.scan()
	return scanner
}

// scan read the underlying reader.
func (s *Scanner) scan() {
	defer close(s.linech)

	for {
		select {
		case <-s.ctx.Done():
			s.err = s.ctx.Err()
			return
		default:
			if !s.Scanner.Scan() {
				return
			}

			s.linech <- s.Scanner.Text()
		}
	}
}

// Scan returns read-only channel to consume lines.
func (s *Scanner) Scan() <-chan string {
	return s.linech
}

// Err returns encountered errors, if any.
func (s *Scanner) Err() error {
	if s.err != nil {
		return s.err
	}

	return s.Scanner.Err()
}

func validateRunCommand(c *cli.Context) error {
	if c.Args().Len() > 1 {
		return fmt.Errorf("expected only 1 file")
	}
	return nil
}
