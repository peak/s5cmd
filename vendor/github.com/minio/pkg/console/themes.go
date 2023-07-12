// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package console

import (
	"sync"

	"github.com/fatih/color"
)

var (
	// Theme contains default color mapping.
	theme = map[string]*color.Color{
		"Debug":  color.New(color.FgWhite, color.Faint, color.Italic),
		"Fatal":  color.New(color.FgRed, color.Italic, color.Bold),
		"Error":  color.New(color.FgYellow, color.Italic),
		"Info":   color.New(color.FgGreen, color.Bold),
		"Print":  color.New(),
		"PrintB": color.New(color.FgBlue, color.Bold),
		"PrintC": color.New(color.FgGreen, color.Bold),
	}

	themeMu sync.Mutex
)

func getThemeColor(tag string) *color.Color {
	themeMu.Lock()
	defer themeMu.Unlock()
	return theme[tag]
}

func setThemeColor(tag string, cl *color.Color) {
	themeMu.Lock()
	defer themeMu.Unlock()
	theme[tag] = cl
}

// SetColorOff disables coloring for the entire session.
func SetColorOff() {
	privateMutex.Lock()
	defer privateMutex.Unlock()
	color.NoColor = true
}

// SetColorOn enables coloring for the entire session.
func SetColorOn() {
	privateMutex.Lock()
	defer privateMutex.Unlock()
	color.NoColor = false
}

// SetColor sets a color for a particular tag.
func SetColor(tag string, cl *color.Color) {
	// add new theme
	setThemeColor(tag, cl)
}
