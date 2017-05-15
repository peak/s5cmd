// Package cmd used for command line options for the complete tool
package cmd

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/posener/complete/cmd/install"
)

// CLI for command line
type CLI struct {
	Name string

	install   bool
	uninstall bool
	yes       bool
}

const (
	defaultInstallName   = "install"
	defaultUninstallName = "uninstall"
)

// Run is used when running complete in command line mode.
// this is used when the complete is not completing words, but to
// install it or uninstall it.
func (f *CLI) Run() bool {

	// add flags and parse them in case they were not added and parsed
	// by the main program
	f.AddFlags(nil, "", "")
	flag.Parse()

	err := f.validate()
	if err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}

	switch {
	case f.install:
		f.prompt()
		err = install.Install(f.Name)
	case f.uninstall:
		f.prompt()
		err = install.Uninstall(f.Name)
	default:
		// non of the action flags matched,
		// returning false should make the real program execute
		return false
	}

	if err != nil {
		fmt.Printf("%s failed! %s\n", f.action(), err)
		os.Exit(3)
	}
	fmt.Println("Done!")
	return true
}

// prompt use for approval
// exit if approval was not given
func (f *CLI) prompt() {
	defer fmt.Println(f.action() + "ing...")
	if f.yes {
		return
	}
	fmt.Printf("%s completion for %s? ", f.action(), f.Name)
	var answer string
	fmt.Scanln(&answer)

	switch strings.ToLower(answer) {
	case "y", "yes":
		return
	default:
		fmt.Println("Cancelling...")
		os.Exit(1)
	}
}

// AddFlags adds the CLI flags to the flag set.
// If flags is nil, the default command line flags will be taken.
// Pass non-empty strings as installName and uninstallName to override the default
// flag names.
func (f *CLI) AddFlags(flags *flag.FlagSet, installName, uninstallName string) {
	if flags == nil {
		flags = flag.CommandLine
	}

	if installName == "" {
		installName = defaultInstallName
	}
	if uninstallName == "" {
		uninstallName = defaultUninstallName
	}

	if flags.Lookup(installName) == nil {
		flags.BoolVar(&f.install, installName, false,
			fmt.Sprintf("Install completion for %s command", f.Name))
	}
	if flags.Lookup(uninstallName) == nil {
		flags.BoolVar(&f.uninstall, uninstallName, false,
			fmt.Sprintf("Uninstall completion for %s command", f.Name))
	}
	if flags.Lookup("y") == nil {
		flags.BoolVar(&f.yes, "y", false, "Don't prompt user for typing 'yes'")
	}
}

// validate the CLI
func (f *CLI) validate() error {
	if f.install && f.uninstall {
		return errors.New("Install and uninstall are mutually exclusive")
	}
	return nil
}

// action name according to the CLI values.
func (f *CLI) action() string {
	switch {
	case f.install:
		return "Install"
	case f.uninstall:
		return "Uninstall"
	default:
		return "unknown"
	}
}
