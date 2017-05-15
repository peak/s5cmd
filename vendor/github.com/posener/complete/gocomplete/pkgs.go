package main

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"strings"

	"github.com/posener/complete"
)

const goListFormat = `'{"name": "{{.Name}}", "dir": "{{.Dir}}"}'`

func predictPackages(packageName string) complete.Predictor {
	return complete.PredictFunc(func(a complete.Args) (prediction []string) {
		dir := a.Directory()
		dir = strings.TrimRight(dir, "/.") + "/..."

		pkgs := listPackages(dir)

		files := make([]string, 0, len(pkgs))
		for _, p := range pkgs {
			if packageName != "" && p.Name != packageName {
				continue
			}
			files = append(files, p.Path)
		}
		return complete.PredictFilesSet(files).Predict(a)
	})
}

type pack struct {
	Name string
	Path string
}

func listPackages(dir string) (pkgs []pack) {
	out, err := exec.Command("go", "list", "-f", goListFormat, dir).Output()
	if err != nil {
		return
	}
	lines := bytes.Split(out, []byte("\n"))
	for _, line := range lines {
		var p pack
		if err := json.Unmarshal(line, &p); err == nil {
			pkgs = append(pkgs, p)
		}
	}
	return
}
