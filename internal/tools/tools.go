//go:build tools

package tools

import (
	_ "go.uber.org/mock/mockgen"
	_ "honnef.co/go/tools/cmd/staticcheck"
	_ "mvdan.cc/unparam"
)
