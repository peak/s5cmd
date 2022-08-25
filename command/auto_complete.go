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

_cli_zsh_autocomplete() {
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

compdef _cli_zsh_autocomplete s5cmd
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
		# We also want to pass COMP_WORDBREAKS to app. Because the application
		# will prepare different suggestions depending on whether COMP_WORDBREAKS
		# contains colons or not.
		opts=$(COMP_WORDBREAKS=$COMP_WORDBREAKS $cmd --generate-bash-completion)

		# prepare completion array with possible values and filter those does not
		# start with cur. if no completion is found then fallback to default completion of shell. 
		while IFS='' read -r line; do COMPREPLY+=("$line"); done < <(compgen -o bashdefault -o default -o nospace -W "${opts}" -- "${cur}")

		return 0
	fi
}

# call the _s5cmd_cli_bash_autocomplete to complete s5cmd command. 
complete  -F _s5cmd_cli_bash_autocomplete s5cmd
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

		if strings.HasPrefix(arg, "s3://") {
			printS3Suggestions(ctx, arg)
		} else {
			cli.DefaultCompleteWithFlags(cmd)(ctx)
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

	if i := strings.LastIndex(argToBeCompleted, ":"); i >= 0 && baseShell == "bash" &&
		strings.Contains(os.Getenv("COMP_WORDBREAKS"), ":") {
		prefix = argToBeCompleted[0 : i+1]
	}
	// fmt.Println("Wb", os.Getenv("COMP_WORDBREAKS"))
	// fmt.Println("prefix", prefix)
	//	fmt.Println("org sug", suggestion)

	suggestion = strings.TrimPrefix(suggestion, prefix)

	//	fmt.Println("new sug", suggestion)
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
