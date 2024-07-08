package command

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
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
		local opts cur cmd` +
	// get current word (cur) and prepare command (cmd)
	`
		cur="${COMP_WORDS[COMP_CWORD]}"
		cmd="${COMP_LINE:0:$COMP_POINT}"` +

	// if we want to complete the second argument and we didn't start writing
	// yet then we should pass an empty string as another argument. Otherwise
	// the white spaces will be discarded and the program will make suggestions
	// as if it is completing the first argument.
	// Beware that we want to pass an empty string so we intentionally write
	// as it is. Fixes of SC2089 and SC2090 are not what we want.
	// see also https://www.shellcheck.net/wiki/SC2090
	`
		[ "${COMP_LINE:COMP_POINT-1:$COMP_POINT}" == " " ] \
			&& cmd="${cmd} ''" ` +

	// execute the command with '--generate-bash-completion' flag to obtain
	// possible completion values for current word.
	// ps. SC2090 is not wanted.
	`
		opts=$($cmd --generate-bash-completion)` +

	// prepare completion array with possible values and filter those do not start with cur.
	// if no completion is found then fallback to default completion of shell.
	`

		while IFS='' read -r line;
		do
			COMPREPLY+=("$line");
		done \
		< <(compgen -o bashdefault -o default -o nospace -W "${opts}" -- "${cur}")

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

func getBashCompleteFn(cmd *cli.Command, isOnlyRemote, isOnlyBucket bool) func(ctx *cli.Context) {
	isOnlyRemote = isOnlyRemote || isOnlyBucket
	return func(ctx *cli.Context) {
		arg := parseArgumentToComplete(ctx)

		if strings.HasPrefix(arg, "-") {
			cli.DefaultCompleteWithFlags(cmd)(ctx)
			return
		}

		if isOnlyRemote || strings.HasPrefix(arg, "s3://") {
			u, err := url.New(arg)
			if err != nil {
				u = &url.URL{Type: 0, Scheme: "s3"}
			}

			c := ctx.Context
			client, err := storage.NewRemoteClient(c, u, NewStorageOpts(ctx))
			if err != nil {
				return
			}

			shell := filepath.Base(os.Getenv("SHELL"))
			printS3Suggestions(c, shell, client, u, arg, isOnlyBucket)
			return
		}
	}
}

// constantCompleteWithDefault returns a complete function which prints the argument, itself, which is to be completed.
// If the argument is empty string it uses the defaultCompletions to make suggestions.
func constantCompleteWithDefault(shell, arg string, defaultCompletions ...string) {
	if arg == "" {
		for _, str := range defaultCompletions {
			fmt.Println(formatSuggestionForShell(shell, str, arg))
		}
	} else {
		fmt.Println(formatSuggestionForShell(shell, arg, arg))
	}
}

func printS3Suggestions(c context.Context, shell string, client *storage.S3, u *url.URL, arg string, isOnlyBucket bool) {
	if u.Bucket == "" || (u.IsBucket() && !strings.HasSuffix(arg, "/")) || isOnlyBucket {
		printListBuckets(c, shell, client, u, arg)
	} else {
		printListNURLSuggestions(c, shell, client, u, 20, arg)
	}
}

func printListBuckets(ctx context.Context, shell string, client *storage.S3, u *url.URL, argToBeCompleted string) {
	buckets, err := client.ListBuckets(ctx, u.Bucket)
	if err != nil {
		return
	}

	for _, bucket := range buckets {
		fmt.Println(formatSuggestionForShell(shell, "s3://"+bucket.Name+"/", argToBeCompleted))
	}
}

func printListNURLSuggestions(ctx context.Context, shell string, client *storage.S3, u *url.URL, count int, argToBeCompleted string) {
	if u.IsBucket() {
		var err error
		u, err = url.New(u.Absolute() + "/")
		if err != nil {
			return
		}
	}

	i := 0
	for obj := range (*client).List(ctx, u, false) {
		if i > count {
			break
		}
		if obj.Err != nil {
			return
		}
		fmt.Println(formatSuggestionForShell(shell, obj.URL.Absolute(), argToBeCompleted))
		i++
	}
}

func printAutocompletionInstructions(shell string) {
	var script string
	baseShell := filepath.Base(shell)
	instructions := `# To enable autocompletion you should add the following script to startup scripts of your shell.
# It is probably located at ~/.` + baseShell + "rc"

	switch baseShell {
	case "zsh":
		script = zsh
	case "bash":
		script = bash
	case "pwsh":
		script = pwsh
		instructions = `# To enable autocompletion you should save the following script to a file named "s5cmd.ps1" and execute it.
# To persist it you should add the path of "s5cmd.ps1" file to profile file (which you can locate with $profile) to automatically execute "s5cmd.ps1" on every shell start up.`
	default:
		instructions = `# We couldn't recognize your SHELL "` + baseShell + `".
# Shell completion is supported only for bash, pwsh and zsh.
# Make sure that your SHELL environment variable is set accurately.`
	}

	fmt.Println(instructions)
	fmt.Println(script)
}

func formatSuggestionForShell(baseShell, suggestion, argToBeCompleted string) string {
	switch baseShell {
	case "bash":
		var prefix string
		suggestions := make([]string, 0, 2)
		if i := strings.LastIndex(argToBeCompleted, ":"); i >= 0 && baseShell == "bash" {
			// include the original suggestion in case that COMP_WORDBREAKS does not contain :
			// or that the argToBeCompleted was quoted.
			// Bash doesn't split on : when argument is quoted even if : is in COMP_WORDBREAKS
			suggestions = append(suggestions, suggestion)
			prefix = argToBeCompleted[0 : i+1]
		}
		suggestions = append(suggestions, strings.TrimPrefix(suggestion, prefix))
		return strings.Join(suggestions, "\n")
	case "zsh":
		// replace every colon : with \:	if shell is zsh
		// colons are used as a seperator for the autocompletion script
		// so "literal colons in completion must be quoted with a backslash"
		// see also https://zsh.sourceforge.io/Doc/Release/Completion-System.html#:~:text=This%20is%20followed,as%20name1%3B
		return strings.ReplaceAll(suggestion, ":", `\:`)
	default:
		return suggestion
	}
}

func parseArgumentToComplete(ctx *cli.Context) string {
	var arg string
	args := ctx.Args()
	l := args.Len()

	if l > 0 {
		arg = args.Get(l - 1)
	}

	// argument may start with a quotation mark, in this case we want to trim
	// that before checking if it has prefix 's3://'.
	// Beware that we only want to trim the first char, not all of the leading
	// quotation marks, because those quotation marks may be actual characters.
	if strings.HasPrefix(arg, "'") {
		arg = strings.TrimPrefix(arg, "'")
	} else {
		arg = strings.TrimPrefix(arg, "\"")
	}
	return arg
}
