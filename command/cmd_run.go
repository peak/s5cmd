package command

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/parallel"
)

var RunCommand = &cli.Command{
	Name:     "run",
	HelpName: "run",
	Usage:    "run commands in batch",
	Before: func(c *cli.Context) error {
		if c.Args().Len() > 1 {
			return fmt.Errorf("expected only 1 file")
		}
		return nil
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

		pm := parallel.New(c.Int("numworkers"))
		defer pm.Close()

		waiter := parallel.NewWaiter()

		var errDoneCh = make(chan bool)
		go func() {
			defer close(errDoneCh)
			for range waiter.Err() {
				// app.ExitErrHandler is called after each command.Run
				// invocation. Ignore the errors returned from parallel.Run,
				// just drain the channel for synchronization.
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

			fields := strings.Fields(line)
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

				cmd := app.Command(subcmd)
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

		return scanner.Err()
	},
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
