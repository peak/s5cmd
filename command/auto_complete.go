package command

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
	"github.com/urfave/cli/v2"
)

const (
	zsh = `autoload -Uz compinit
compinit

_cli_zsh_autocomplete() {
	local -a opts
	local cur
	cur=${words[-1]}
	opts=("${(@f)$(${words[@]:0:#words[@]-1} ${cur} --generate-bash-completion)}")
  
	if [[ "${opts[1]}" != "" ]]; then
	  _describe 'values' opts
	else
	  _files
	fi
  }
  
  compdef _cli_zsh_autocomplete s5cmd
`
	// NOTE: Broken, WIP. Requires `bash-completion` to be installed/sourced;
	//	- https://github.com/scop/bash-completion
	bash = `_s5cmd_cli_bash_autocomplete() {

# get current word and its index (cur and cword respectively),
# and prepare command (cmd)
# exclude : from the word breaks
local cur
cur="${COMP_WORDS[COMP_CWORD]}"
cmd="${COMP_LINE:0:$COMP_POINT}"

echo cmd "$cmd" >> log.txt

if [[ "${COMP_WORDS[0]}" != "source" ]]; then
	COMPREPLY=()
	# execute the command with '--generate-bash-completion' flag to obtain
	# possible completion values for current word
	local opts=$(${cmd} --generate-bash-completion)


	# prepare completion array with possible values and filter those does not
	# start with cur if no completion is found then fallback to default completion of shell. 
	COMPREPLY=($(compgen -o bashdefault -o default -o nospace -W "${opts}" -- ${cur}))

	return 0
fi
	}

# call the _s5cmd_cli_bash_autocomplete to complete s5cmd command. 
complete  -F _s5cmd_cli_bash_autocomplete s5cmd
`
	powershell = `$fn = $($MyInvocation.MyCommand.Name)
	  $name = $fn -replace "(.*)\.ps1$", '$1'
	  Register-ArgumentCompleter -Native -CommandName $name -ScriptBlock {
		   param($commandName, $wordToComplete, $cursorPosition)
		   $other = "$wordToComplete --generate-bash-completion"
			   Invoke-Expression $other | ForEach-Object {
				  [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_)
			   }
	   }
	  Footer
	  `
)

func getBashCompleteFn(cmd *cli.Command) func(ctx *cli.Context) {
	return func(ctx *cli.Context) {
		var arg string
		args := ctx.Args()
		l := args.Len()
		f, _ := os.Open("log.txt")
		defer f.Close()
		f.WriteString(fmt.Sprint("this//arg=", args, "l=", l))

		if l > 2 && filepath.Base(os.Getenv("SHELL")) == "bash" {
			arg = args.Get(l-3) + args.Get(l-2) + args.Get(l-1)
		} else if l > 0 {
			arg = args.Get(l - 1)
		}
		if strings.HasPrefix(arg, "s3://") {
			printS3Suggestions(ctx, arg)
		} else {
			cli.DefaultCompleteWithFlags(cmd)(ctx)
		}
		f.WriteString(escapeColon(fmt.Sprint("\n//arg=", arg, "l=", l)))
	}
}

func printS3Suggestions(ctx *cli.Context, arg string) {
	c := ctx.Context
	u, err := url.New(arg)
	if err != nil {
		u = &url.URL{Type: 0, Scheme: "s3"}
	}
	client, err := storage.NewRemoteClient(c, u, NewStorageOpts(ctx))
	if err != nil {
		return
	}

	if u.Bucket == "" || (u.IsBucket() && !strings.HasSuffix(arg, "/")) {
		printListBuckets(c, client, u)
	} else {
		prefix := ""
		if i := strings.LastIndex(arg, ":"); i >= 0 { //os.Getenv("COMP_WORDBREAKS"))
			prefix = arg[0 : i+1]
		}

		printListNURLSuggestions(c, client, u, 13, prefix)
	}
}

func printListBuckets(ctx context.Context, client *storage.S3, u *url.URL) {
	buckets, err := client.ListBuckets(ctx, u.Bucket)
	if err != nil {
		return
	}

	for _, bucket := range buckets {
		if filepath.Base(os.Getenv("SHELL")) == "bash" {
			fmt.Println(escapeColon("//" + bucket.Name + "/"))
		} else {
			fmt.Println(escapeColon("s3://" + bucket.Name + "/"))
		}
	}
}

func printListNURLSuggestions(ctx context.Context, client *storage.S3, u *url.URL, count int, prefix string) {
	abs := u.Absolute()
	if u.IsBucket() {
		abs = abs + "/"
	}
	u, err := url.New(abs)
	if err != nil {
		return
	}

	i := 0
	for obj := range (*client).List(ctx, u, false) {
		if i > count {
			break
		}
		if obj.Err != nil {
			return
		}
		if filepath.Base(os.Getenv("SHELL")) == "bash" {
			fmt.Println(escapeColon(strings.TrimPrefix(obj.URL.Absolute(), prefix)))
		} else {
			fmt.Println(escapeColon(obj.URL.Absolute()))
		}

		i++
	}
}

func installCompletionHelp(shell string) {
	baseShell := filepath.Base(shell)
	fmt.Println("# To enable autocompletion you should add the following script to startup scripts of your shell.")
	if baseShell != "" {
		fmt.Println("# It is probably located at ~/." + baseShell + "rc")
	}
	var script string
	if baseShell == "zsh" {
		script = zsh
	} else if baseShell == "bash" {
		script = bash
	} else if baseShell == "powershell" {
		script = powershell
	} else {
		script = "# Your shell \"" + baseShell + "\" is not recognized. Auto complete is only available with zsh, bash and powershell (?)."
	}

	fmt.Println(script)
}

// replace every colon : with \: if shell is zsh
// colons are used as a seperator for the autocompletion script
// so "literal colons in completion must be quoted with a backslash"
// see also https://zsh.sourceforge.io/Doc/Release/Completion-System.html#:~:text=This%20is%20followed,as%20name1%3B
func escapeColon(str ...interface{}) string {
	baseShell := filepath.Base(os.Getenv("SHELL"))

	if baseShell == "zsh" {
		return strings.ReplaceAll(fmt.Sprint(str...), ":", `\:`)
	}

	return fmt.Sprint(str...)
}
