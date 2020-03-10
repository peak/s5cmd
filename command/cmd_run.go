package command

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
)

var RunCommand = &cli.Command{
	Name:     "run",
	HelpName: "run",
	Usage:    "TODO",
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

			fields = append([]string{"s5cmd"}, fields...)

			if err := app.Run(fields); err != nil {
				fmt.Println("ERR:", err)
			}
		}

		return scanner.Err()
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
