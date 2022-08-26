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

const zsh = `autoload -Uz compinit
compinit

_s5cmd_cli_zsh_autocomplete() {
	local -a opts
	local cur
	cur=${words[-1]}
	opts=("${(@f)$(${words[@]:0:#words[@]-1} "${cur}" --generate-bash-completion)}")

	if [[ "${opts[1]}" != "" ]]; then
	  _describe 'values' opts
	else
	  _files
	fi
}

compdef _s5cmd_cli_zsh_autocomplete s5cmd
`

const bash = `# prepare autocompletion suggestions for s5cmd and save them to COMPREPLY array
_s5cmd_cli_bash_autocomplete() {

	if [[ "${COMP_WORDS[0]}" != "source" ]]; then
		COMPREPLY=()
		local opts cur cmd

		# get current word (cur) and prepare command (cmd)
		cur="${COMP_WORDS[COMP_CWORD]}"
		cmd="${COMP_LINE:0:$COMP_POINT}"

		# if we want to complete the second argurment and we didn't started writing
		# yet then we should pass an empty string as another argument. Otherwise
		# the white spaces will be discarded and the program will make suggestions
		# as if it is completing the first argument.
		# shellcheck disable=SC2089,SC2090
		# Beware that the we want to pass empty string so we intentionally write
		# as it is. Fixes of SC2089 and SC2090 are not what we want.
		# see also https://www.shellcheck.net/wiki/SC2090
		[ "${COMP_LINE:COMP_POINT-1:$COMP_POINT}" == " " ] \
			&& [ "${COMP_LINE:COMP_POINT-2:$COMP_POINT}" != '\ ' ] \
			&& cmd="${cmd} \"\"" 

		# execute the command with '--generate-bash-completion' flag to obtain
		# possible completion values for current word.
		# shellcheck disable=SC2090
		opts=$($cmd --generate-bash-completion)

		# prepare completion array with possible values and filter those does not
		# start with cur. if no completion is found then fallback to default completion of shell. 
		while IFS='' read -r line; do COMPREPLY+=("$line"); done < <(compgen -o bashdefault -o default -o nospace -W "${opts}" -- "${cur}")

		return 0
	fi
}

# call the _s5cmd_cli_bash_autocomplete to complete s5cmd command. 
complete -o nospace -F _s5cmd_cli_bash_autocomplete s5cmd
`

const pwsh = `$fn = $($MyInvocation.MyCommand.Name)
$name = $fn -replace "(.*)\.ps1$", '$1'
Register-ArgumentCompleter -Native -CommandName $name -ScriptBlock {
	param($commandName, $wordToComplete, $cursorPosition)
	$other = "$wordToComplete --generate-bash-completion"
		Invoke-Expression $other | ForEach-Object {
		[System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_)
		}
}
`

func getBashCompleteFn(cmd *cli.Command) func(ctx *cli.Context) {
	return func(ctx *cli.Context) {
		var arg string
		args := ctx.Args()
		l := args.Len()

		if l > 0 {
			arg = args.Get(l - 1)
		}

		// argument may start with a quotation mark, in this case we want to trim that before
		// checking if it has prefix s3://
		// Beware that we only want to trim the first char, not all of the leading
		// quotation marks, because those quotation marks may be actual charactes.
		if strings.HasPrefix(arg, "'") {
			arg = strings.TrimPrefix(arg, "'")
		} else {
			arg = strings.TrimPrefix(arg, "\"")
		}

		if strings.HasPrefix(arg, "s3://") {
			printS3Suggestions(ctx, arg)
		} else {
			cli.DefaultCompleteWithFlags(cmd)(ctx)
		}
	}
}

func getRemoteCompleteFn(cmd *cli.Command) func(ctx *cli.Context) {
	return func(ctx *cli.Context) {
		var arg string
		args := ctx.Args()
		l := args.Len()

		if l > 0 {
			arg = args.Get(l - 1)
		}

		if strings.HasPrefix(arg, "-") {
			cli.DefaultCompleteWithFlags(cmd)(ctx)
		} else if !strings.HasPrefix(arg, "s3://") {
			arg = "s3://"
		}
		printS3Suggestions(ctx, arg)
	}
}

// it returns a complete function which prints the argument, itself, which is to be completed.
// If the argument is empty string it uses the defaultCompletions to make suggestions.
func ineffectiveCompleteFnWithDefault(cmd *cli.Command, defaultCompletions ...string) func(ctx *cli.Context) {
	return func(ctx *cli.Context) {
		var arg string
		args := ctx.Args()
		if args.Len() > 0 {
			arg = args.Get(args.Len() - 1)
		}
		if arg == "" {
			for _, str := range defaultCompletions {
				fmt.Println(formatSuggestionForShell(str, str))
			}
		} else if strings.HasPrefix(arg, "-") {
			cli.DefaultCompleteWithFlags(cmd)(ctx)
		} else {
			fmt.Println(formatSuggestionForShell(arg, arg))
		}
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
		fmt.Fprintln(os.Stderr, err)
		return
	}

	if u.Bucket == "" || (u.IsBucket() && !strings.HasSuffix(arg, "/")) {
		printListBuckets(c, client, u, arg)
	} else {
		printListNURLSuggestions(c, client, u, 13, arg)
	}
}

func printListBuckets(ctx context.Context, client *storage.S3, u *url.URL, argToBeCompleted string) {
	buckets, err := client.ListBuckets(ctx, u.Bucket)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	for _, bucket := range buckets {
		fmt.Println(formatSuggestionForShell("s3://"+bucket.Name+"/", argToBeCompleted))
	}
}

func printListNURLSuggestions(ctx context.Context, client *storage.S3, u *url.URL, count int, argToBeCompleted string) {
	abs := u.Absolute()
	if u.IsBucket() {
		abs = abs + "/"
	}
	u, err := url.New(abs)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	i := 0
	for obj := range (*client).List(ctx, u, false) {
		if i > count {
			break
		}
		if obj.Err != nil {
			fmt.Fprintln(os.Stderr, obj.Err)
			return
		}
		fmt.Println(formatSuggestionForShell(obj.URL.Absolute(), argToBeCompleted))
		i++
	}
}

func installCompletionHelp(shell string) {
	var script string
	baseShell := filepath.Base(shell)
	instructions := "# To enable autocompletion you should add the following script" +
		" to startup scripts of your shell.\n" +
		"# It is probably located at ~/." + baseShell + "rc"

	if baseShell == "zsh" {
		script = zsh
	} else if baseShell == "bash" {
		script = bash
	} else if baseShell == "pwsh" {
		script = pwsh
		instructions = "# To enable autocompletion you should save the following" +
			" script to a file named \"s5cmd.ps1\" and execute it.\n# To persist it" +
			" you should add the path of \"s5cmd.ps1\" file to profile file " +
			"(which you can locate with $profile) to automatically execute \"s5cmd.ps1\"" +
			" on every shell start up."
	} else {
		instructions = "# We couldn't recognize your SHELL \"" + baseShell + "\".\n" +
			"# Shell completion is supported only for bash, pwsh and zsh." +
			"# Make sure that your SHELL environment variable is set accurately."
	}

	fmt.Println(instructions)
	fmt.Println(script)
}

func formatSuggestionForShell(suggestion, argToBeCompleted string) string {
	var prefix string
	baseShell := filepath.Base(os.Getenv("SHELL"))

	if i := strings.LastIndex(argToBeCompleted, ":"); i >= 0 && baseShell == "bash" {
		// write the original suggestion in case that the argToBeCompleted was quoted.
		// Bash doesn't split on : when argument is quoted even if : is in COMP_WORDBREAKS
		fmt.Println(suggestion)
		prefix = argToBeCompleted[0 : i+1]
	}

	suggestion = strings.TrimPrefix(suggestion, prefix)

	// replace every colon : with \:	if shell is zsh
	// colons are used as a seperator for the autocompletion script
	// so "literal colons in completion must be quoted with a backslash"
	// see also https://zsh.sourceforge.io/Doc/Release/Completion-System.html#:~:text=This%20is%20followed,as%20name1%3B
	if baseShell == "zsh" {
		suggestion = escapeColon(suggestion)
	}
	return suggestion
}

// replace every colon : with \:
func escapeColon(str ...interface{}) string {
	return strings.ReplaceAll(fmt.Sprint(str...), ":", `\:`)
}
