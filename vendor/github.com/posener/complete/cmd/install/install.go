package install

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"

	"github.com/hashicorp/go-multierror"
)

type installer interface {
	Install(cmd, bin string) error
	Uninstall(cmd, bin string) error
}

// Install complete command given:
// cmd: is the command name
func Install(cmd string) error {
	is := installers()
	if len(is) == 0 {
		return errors.New("Did not found any shells to install")
	}
	bin, err := getBinaryPath()
	if err != nil {
		return err
	}

	for _, i := range is {
		errI := i.Install(cmd, bin)
		if errI != nil {
			multierror.Append(err, errI)
		}
	}

	return err
}

// Uninstall complete command given:
// cmd: is the command name
func Uninstall(cmd string) error {
	is := installers()
	if len(is) == 0 {
		return errors.New("Did not found any shells to uninstall")
	}
	bin, err := getBinaryPath()
	if err != nil {
		return err
	}

	for _, i := range is {
		errI := i.Uninstall(cmd, bin)
		if errI != nil {
			multierror.Append(err, errI)
		}
	}

	return err
}

func installers() (i []installer) {
	if f := rcFile(".bashrc"); f != "" {
		i = append(i, bash{f})
	}
	if f := rcFile(".zshrc"); f != "" {
		i = append(i, zsh{f})
	}
	return
}

func getBinaryPath() (string, error) {
	bin, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Abs(bin)
}

func rcFile(name string) string {
	u, err := user.Current()
	if err != nil {
		return ""
	}
	return filepath.Join(u.HomeDir, name)
}
