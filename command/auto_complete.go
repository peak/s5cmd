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
	bash = `
_cli_bash_autocomplete() {

	local cur prev words cword split
	_init_completion -n : -s || return

	# print cur prev words cword split
    echo '---------' >> bash.log
	echo "cur: ${cur}" >> bash.log
	echo "prev: ${prev}" >> bash.log
	echo "words: ${words[*]}" >> bash.log
	echo "cword: ${cword}" >> bash.log
	echo "split: ${split}" >> bash.log
    echo "cmd : ${words[@]:0:$cword}" >> bash.log
    echo '---------' >> bash.log
    local cmd="${words[@]:0:$cword}"

	if [[ "${COMP_WORDS[0]}" != "source" ]]; then
		COMPREPLY=()
		local opts=$(${cmd} ${cur} --generate-bash-completion)

		echo "opts: ${opts}" >>bash.log
		COMPREPLY=($(compgen -W "${opts}" -- ${cur}))
        __ltrim_colon_completions "$cur"
		return 0
	fi
}
complete -o bashdefault -o default -o nospace -F _cli_bash_autocomplete s5cmd
`

	pwsh = `$fn = $($MyInvocation.MyCommand.Name)
$name = $fn -replace "(.*)\.ps1$", '$1'
Register-ArgumentCompleter -Native -CommandName $name -ScriptBlock {
	param($commandName, $wordToComplete, $cursorPosition)
	$other = "$wordToComplete --generate-bash-completion"
	Invoke-Expression $other | ForEach-Object {
		[System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_)
	}
}`
)

func getBashCompleteFn(cmd *cli.Command) func(ctx *cli.Context) {
	return func(ctx *cli.Context) {
		var arg string
		args := ctx.Args()
		if args.Len() > 0 {
			arg = args.Get(args.Len() - 1)
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
		printListBuckets(c, client, u)
	} else {
		printListNURLSuggestions(c, client, u, 20)
	}
}

func printListBuckets(ctx context.Context, client *storage.S3, u *url.URL) {
	buckets, err := client.ListBuckets(ctx, u.Bucket)
	if err != nil {
		return
	}

	for _, bucket := range buckets {
		fmt.Println(escapeColon("s3://" + bucket.Name))
	}
}

func printListNURLSuggestions(ctx context.Context, client *storage.S3, u *url.URL, count int) {
	abs := u.Absolute()
	if u.IsBucket() {
		abs = abs + "/"
	}
	u, err := url.New(abs + "*")
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
		fmt.Println(escapeColon(obj.URL.Absolute()))

		i++
	}
}

func installCompletionHelp(shell string) {
	baseShell := filepath.Base(shell)

	instructions := "# To enable autocompletion you should add the following script" +
		" to startup scripts of your shell.\n" +
		"# It is probably located at ~/." + baseShell + "rc"
	var script string
	if baseShell == "zsh" {
		script = zsh
	} else if baseShell == "bash" {
		script = bash
	} else if baseShell == "pwsh" {
		script = pwsh
		instructions = "# To enable autocompletion you should save the following" +
			" script to a file named \"s5cmd.ps1\" and execute it.\n# To persist it" +
			" you should add a line to profile file (which you can locate with $profile)" +
			" to execute  \"s5cmd.ps1\"."
	} else {
		instructions = "# We couldn't recognize your SHELL \"" + baseShell + "\".\n" +
			"# Shell completion is supported only for bash, pwsh and zsh."
	}

	fmt.Println(instructions)
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
