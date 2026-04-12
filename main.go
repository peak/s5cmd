package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/peak/s5cmd/v2/command"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// intCh receives only SIGINT so we know whether the termination was
	// caused by Ctrl-C (as opposed to SIGTERM or a command error).  It
	// must be registered *before* command.Main is called so the signal is
	// captured even if the command returns very quickly.
	intCh := make(chan os.Signal, 1)
	signal.Notify(intCh, os.Interrupt)
	defer signal.Stop(intCh)

	err := command.Main(ctx, os.Args)

	// Check non-blocking: if the channel already has a value, SIGINT arrived
	// while the command was running.
	select {
	case <-intCh:
		// SIGINT was the cause; exit 130 per POSIX convention (128 + 2).
		os.Exit(130)
	default:
	}

	if err != nil {
		os.Exit(1)
	}
}
