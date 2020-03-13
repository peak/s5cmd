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
	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/parallel"
)

var RunCommand = &cli.Command{
	Name:     "run",
	HelpName: "run",
	Usage:    "TODO",
	Before: func(c *cli.Context) error {
		validate := func() error {
			if c.Args().Len() > 1 {
				return fmt.Errorf("expected only 1 file")
			}
			return nil
		}
		if err := validate(); err != nil {
			printError(givenCommand(c), c.Command.Name, err)
			return err
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

		var merror error
		go func() {
			for err := range waiter.Err() {
				merror = multierror.Append(merror, err)
			}
		}()

		scanner := NewScanner(c.Context, reader)
		for line := range scanner.Scan() {
			fields := strings.Fields(line)

			if len(fields) == 0 {
				fmt.Println("ERR: invalid command")
				continue
			}

			if fields[0] == "run" {
				fmt.Println("ERR: no sir!")
				continue
			}

			fn := func() error {
				subcmd := fields[0]

				flagset := flag.NewFlagSet(subcmd, flag.ExitOnError)
				if err := flagset.Parse(fields); err != nil {
					return err
				}

				cmd := app.Command(subcmd)
				cmdctx := cli.NewContext(app, flagset, c)
				return cmd.Run(cmdctx)
			}
			pm.Run(fn, waiter)
		}

		waiter.Wait()

		if err := scanner.Err(); err != nil {
			return err
		}

		return merror
	},
}

// Scanner is a cancelable scanner.
type Scanner struct {
	*bufio.Scanner
	err  error
	data chan string
	ctx  context.Context
}

// NewScanner creates a new scanner with cancellation.
func NewScanner(ctx context.Context, r io.Reader) *Scanner {
	scanner := &Scanner{
		ctx:     ctx,
		Scanner: bufio.NewScanner(r),
		data:    make(chan string),
	}

	go scanner.scan()
	return scanner
}

// scan read the underlying reader.
func (s *Scanner) scan() {
	defer close(s.data)

	for {
		select {
		case <-s.ctx.Done():
			s.err = s.ctx.Err()
			return
		default:
			if !s.Scanner.Scan() {
				return
			}

			s.data <- s.Scanner.Text()
		}
	}
}

// Scan returns read-only channel to consume lines.
func (s *Scanner) Scan() <-chan string {
	return s.data
}

// Err returns error of scanner.
func (s *Scanner) Err() error {
	if s.err != nil {
		return s.err
	}

	return s.Scanner.Err()
}
