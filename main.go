package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/afontani/s5cmd/command"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		cancel()
		signal.Stop(ch)
	}()

	if err := command.Main(ctx, os.Args); err != nil {
		os.Exit(1)
	}
}
