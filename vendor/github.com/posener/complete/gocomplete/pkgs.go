package main

import (
	"go/build"
	"io/ioutil"
	"path/filepath"

	"github.com/posener/complete"
)

// predictPackages completes packages in the directory pointed by a.Last
// and packages that are one level below that package.
func predictPackages(a complete.Args) (prediction []string) {
	prediction = complete.PredictFilesSet(listPackages(a.Directory())).Predict(a)
	if len(prediction) != 1 {
		return
	}
	return complete.PredictFilesSet(listPackages(prediction[0])).Predict(a)
}

// listPackages looks in current pointed dir and in all it's direct sub-packages
// and return a list of paths to go packages.
func listPackages(dir string) (directories []string) {
	// add subdirectories
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		complete.Log("failed reading directory %s: %s", dir, err)
		return
	}

	// build paths array
	paths := make([]string, 0, len(files)+1)
	for _, f := range files {
		if f.IsDir() {
			paths = append(paths, filepath.Join(dir, f.Name()))
		}
	}
	paths = append(paths, dir)

	// import packages according to given paths
	for _, p := range paths {
		pkg, err := build.ImportDir(p, 0)
		if err != nil {
			complete.Log("failed importing directory %s: %s", p, err)
			continue
		}
		directories = append(directories, pkg.Dir)
	}
	return
}
