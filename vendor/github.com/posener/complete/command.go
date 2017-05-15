package complete

import "github.com/posener/complete/match"

// Command represents a command line
// It holds the data that enables auto completion of command line
// Command can also be a sub command.
type Command struct {
	// Sub is map of sub commands of the current command
	// The key refer to the sub command name, and the value is it's
	// Command descriptive struct.
	Sub Commands

	// Flags is a map of flags that the command accepts.
	// The key is the flag name, and the value is it's predictions.
	Flags Flags

	// Args are extra arguments that the command accepts, those who are
	// given without any flag before.
	Args Predictor
}

// Predict returns all possible predictions for args according to the command struct
func (c *Command) Predict(a Args) (predictions []string) {
	predictions, _ = c.predict(a)
	return
}

// Commands is the type of Sub member, it maps a command name to a command struct
type Commands map[string]Command

// Predict completion of sub command names names according to command line arguments
func (c Commands) Predict(a Args) (prediction []string) {
	for sub := range c {
		if match.Prefix(sub, a.Last) {
			prediction = append(prediction, sub)
		}
	}
	return
}

// Flags is the type Flags of the Flags member, it maps a flag name to the flag predictions.
type Flags map[string]Predictor

// Predict completion of flags names according to command line arguments
func (f Flags) Predict(a Args) (prediction []string) {
	for flag := range f {
		if match.Prefix(flag, a.Last) {
			prediction = append(prediction, flag)
		}
	}
	return
}

// predict options
// only is set to true if no more options are allowed to be returned
// those are in cases of special flag that has specific completion arguments,
// and other flags or sub commands can't come after it.
func (c *Command) predict(a Args) (options []string, only bool) {

	// if wordCompleted has something that needs to follow it,
	// it is the most relevant completion
	if predictor, ok := c.Flags[a.LastCompleted]; ok && predictor != nil {
		Log("Predicting according to flag %s", a.Last)
		return predictor.Predict(a), true
	}

	// search sub commands for predictions
	subCommandFound := false
	for i, arg := range a.Completed {
		if cmd, ok := c.Sub[arg]; ok {
			subCommandFound = true

			// recursive call for sub command
			options, only = cmd.predict(a.from(i))
			if only {
				return
			}
		}
	}

	// if no sub command was found, return a list of the sub commands
	if !subCommandFound {
		options = append(options, c.Sub.Predict(a)...)
	}

	// add global available complete options
	options = append(options, c.Flags.Predict(a)...)

	// add additional expected argument of the command
	if c.Args != nil {
		options = append(options, c.Args.Predict(a)...)
	}

	return
}
