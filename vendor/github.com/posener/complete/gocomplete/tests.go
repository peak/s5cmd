package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"

	"github.com/posener/complete"
	"github.com/posener/complete/match"
)

// predictTest predict test names.
// it searches in the current directory for all the go test files
// and then all the relevant function names.
// for test names use prefix of 'Test' or 'Example', and for benchmark
// test names use 'Benchmark'
func predictTest(funcPrefix ...string) complete.Predictor {
	return complete.PredictFunc(func(a complete.Args) (prediction []string) {
		tests := testNames(funcPrefix)
		for _, t := range tests {
			if match.Prefix(t, a.Last) {
				prediction = append(prediction, t)
			}
		}
		return
	})
}

// get all test names in current directory
func testNames(funcPrefix []string) (tests []string) {
	filepath.Walk("./", func(path string, info os.FileInfo, err error) error {
		// if not a test file, skip
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		// inspect test file and append all the test names
		tests = append(tests, testsInFile(funcPrefix, path)...)
		return nil
	})
	return
}

func testsInFile(funcPrefix []string, path string) (tests []string) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		complete.Log("Failed parsing %s: %s", path, err)
		return nil
	}
	for _, d := range f.Decls {
		if f, ok := d.(*ast.FuncDecl); ok {
			name := f.Name.String()
			for _, prefix := range funcPrefix {
				if strings.HasPrefix(name, prefix) {
					tests = append(tests, name)
					break
				}
			}
		}
	}
	return
}
