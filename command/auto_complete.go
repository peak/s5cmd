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
    if [[ "${COMP_WORDS[0]}" != "source" ]]; then
        local cur opts base;
        COMPREPLY=();
        cur="${COMP_WORDS[COMP_CWORD]}";
        opts=$(${COMP_WORDS[@]:0:$COMP_CWORD} ${cur} --generate-bash-completion);
        COMPREPLY=($(compgen -W "${opts}"));
        return 0;
    fi
}

complete -o bashdefault -o default -o nospace -F _cli_bash_autocomplete s5cmd
`
	/*
	   _cli_bash_autocomplete() {
	   		if [[ "${COMP_WORDS[0]}" != "source" ]]; then
	   		  local cur opts base
	   		  COMPREPLY=()
	   		  cur="${COMP_WORDS[COMP_CWORD]}"
	   			opts=$(${COMP_LINE} --generate-bash-completion)
	         # echo  "!
	         #  cur id $cur
	         #  opts are ${opts}.
	         # !"

	       # add each line as an element
	         while IFS='' read -r line; do COMPREPLY+=("$line"); done < <(compgen -W "${

	   #		  COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
	         # echo  "§
	         # COMPREPLY =" "${COMPREPLY[@]}" "
	         # §"
	   		  return 0
	   		fi
	   	  }

	   	  complete -o bashdefault -o default -o nospace -F _cli_bash_autocomplete s5cmd
	*/
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
		printListNURLSuggestions(c, client, u, 13)
	}
}

func printListBuckets(ctx context.Context, client *storage.S3, u *url.URL) {
	buckets, err := client.ListBuckets(ctx, u.Bucket)
	if err != nil {
		return
	}

	for _, bucket := range buckets {
		if filepath.Base(os.Getenv("SHELL")) == "bash" {
			fmt.Println(escapeColon("//" + bucket.Name))
		} else {
			fmt.Println(escapeColon("s3://" + bucket.Name))
		}
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
		if filepath.Base(os.Getenv("SHELL")) == "bash" {
			fmt.Println(escapeColon(strings.TrimPrefix(obj.URL.Absolute(), "s3:")))
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
