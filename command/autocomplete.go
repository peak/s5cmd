package command

import (
	"github.com/posener/complete"
	"github.com/urfave/cli/v2"
)

func adaptCommand(cmd *cli.Command) complete.Command {
	return complete.Command{
		Flags: adaptFlags(cmd.Flags),
		// TODO(ig): add args predictors
	}
}

func adaptFlags(flags []cli.Flag) complete.Flags {
	completionFlags := make(complete.Flags)

	for _, flag := range flags {
		for _, flagname := range flag.Names() {
			if len(flagname) == 1 {
				flagname = "-" + flagname
			} else {
				flagname = "--" + flagname
			}
			completionFlags[flagname] = complete.PredictNothing
		}
	}
	return completionFlags
}

func maybeAutoComplete() bool {
	cmpCommands := make(complete.Commands)
	for _, cmd := range app.Commands {
		cmpCommands[cmd.Name] = adaptCommand(cmd)
	}

	completionCmd := complete.Command{
		Flags: adaptFlags(app.Flags),
		Sub:   cmpCommands,
	}
	return complete.New(appName, completionCmd).Complete()
}
