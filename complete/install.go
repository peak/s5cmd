package complete

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
)

//
// This code is largely inspired by the github.com/posener/complete/cmd/install package
//
// + On install, will add COMP_WORDBREAKS if it's not already there
// + On uninstall, remove only what's added and nothing else
// - On uninstall, will not save/backup the unmodified file (FIXME)
// - Less modular/magical, no interfaces etc.
//

type shellSetupType struct {
	configFile    string
	requiredLines []string
}

var shellSetups = map[string]shellSetupType{
	"bash": {
		configFile: ".bashrc",
		requiredLines: []string{
			"COMP_WORDBREAKS=${COMP_WORDBREAKS//:}",
			"complete -C %binary %command",
		},
	},
	"zsh": {
		configFile: ".zshrc",
		requiredLines: []string{
			// From http://blogs.perl.org/users/perlancar/2014/11/comparing-programmable-tab-completion-in-bash-zsh-tcsh-and-fish.html
			`_s5cmd_completer() { read -l; local cl="$REPLY"; read -ln; local cp="$REPLY"; reply=(` + "`" + `COMP_SHELL=zsh COMP_LINE="$cl" COMP_POINT="$cp" %binary` + "`" + `) }`,
			`compctl -K _s5cmd_completer s5cmd`,
		},
	},
}

const (
	setupPrefix  = "# start s5cmd -- Lines below are added by s5cmd -cmp-install"
	setupComment = "# To automatically uninstall, do not remove these comments and run s5cmd -cmp-uninstall"
	setupSuffix  = "# end s5cmd"
	setupRegex   = `(?ms)(^\n?# start s5cmd.+?# end s5cmd\n?$)`
)

func configFiles() (ret []string) {
	for _, se := range shellSetups {
		ret = append(ret, se.configFile)
	}
	return ret
}

func prepareLine(line, binary string) string {
	line = strings.Replace(line, "%binary", binary, -1)
	line = strings.Replace(line, "%command", "s5cmd", -1)
	return line
}

func setupCompletion(install bool) error {
	verb := "install"
	if !install {
		verb = "uninstall"
	}

	binPath, err := getBinaryPath()
	if err != nil {
		return err
	}

	promptUser := flag.Arg(0) != "assume-yes" // We don't add the flag as a flag, so that it won't clutter the help text
	foundOne := false
	takenAction := false
	for shellName, setup := range shellSetups {
		cfgFile := findExistingFileInHomeDir(setup.configFile)
		if cfgFile == "" {
			fmt.Printf("Could not find %s, skipping for %s\n", setup.configFile, shellName)
			continue
		}
		foundOne = true

		var linesMissing []string
		for _, line := range setup.requiredLines {
			line = prepareLine(line, binPath)
			if !lineInFile(cfgFile, line) {
				linesMissing = append(linesMissing, line)
			}
		}

		if install && len(linesMissing) == 0 {
			fmt.Printf("Already installed in %s\n", setup.configFile)
			continue
		}
		if !install {
			if len(linesMissing) == len(setup.requiredLines) {
				fmt.Printf("Already not installed in %s\n", setup.configFile)
				continue
			}

			prefixFound := lineInFile(cfgFile, setupPrefix)
			suffixFound := lineInFile(cfgFile, setupSuffix)
			if !prefixFound || !suffixFound {
				fmt.Printf("Could not find automatic comments in %s, will not auto-uninstall for %s\n", setup.configFile, shellName)
				continue
			}
		}

		if promptUser {
			fmt.Printf("%s shell completion for %s? This will modify your ~/%s [y/N] ", strings.Title(verb), shellName, setup.configFile)
			var answer string
			fmt.Scanln(&answer)
			switch strings.ToLower(answer) {
			case "y", "yes":
				// no-op
			default:
				continue
			}
		}

		if install {
			linesToAdd := []string{setupPrefix, setupComment}
			linesToAdd = append(linesToAdd, linesMissing...)
			linesToAdd = append(linesToAdd, setupSuffix)
			appendToFile(cfgFile, strings.Join(linesToAdd, "\n"))
			fmt.Printf("Installed for %s\n", shellName)
		} else {
			r := regexp.MustCompile(setupRegex)

			contents, err := ioutil.ReadFile(cfgFile)
			if err != nil {
				return err
			}

			newContents := r.ReplaceAll(contents, nil)
			if len(newContents) == len(contents) {
				fmt.Printf("Error processing %s: regex did not match the comments\n", setup.configFile)
				continue
			}
			err = ioutil.WriteFile(cfgFile, newContents, os.ModePerm)
			if err != nil {
				fmt.Printf("Error processing %s: %s\n", setup.configFile, err.Error())
				continue
			} else {
				fmt.Printf("Uninstalled from %s\n", setup.configFile)
			}
		}

		takenAction = true
	}
	if !foundOne {
		return fmt.Errorf("could not find %s in home directory", strings.Join(configFiles(), " or "))
	}
	if !takenAction {
		fmt.Println("No action taken")
	}
	return nil
}

func findExistingFileInHomeDir(name string) string {
	u, err := user.Current()
	if err != nil {
		return ""
	}
	fn := filepath.Join(u.HomeDir, name)
	st, err := os.Stat(fn)
	if err != nil {
		return ""
	}
	if st.IsDir() {
		return ""
	}
	return fn
}

func getBinaryPath() (string, error) {
	bin, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Abs(bin)
}
