// Package main is complete tool for the go command line
package main

import "github.com/posener/complete"

var (
	ellipsis   = complete.PredictSet("./...")
	anyPackage = complete.PredictFunc(predictPackages)
	goFiles    = complete.PredictFiles("*.go")
	anyFile    = complete.PredictFiles("*")
	anyGo      = complete.PredictOr(goFiles, anyPackage, ellipsis)
)

func main() {
	build := complete.Command{
		Flags: complete.Flags{
			"-o": anyFile,
			"-i": complete.PredictNothing,

			"-a":             complete.PredictNothing,
			"-n":             complete.PredictNothing,
			"-p":             complete.PredictAnything,
			"-race":          complete.PredictNothing,
			"-msan":          complete.PredictNothing,
			"-v":             complete.PredictNothing,
			"-work":          complete.PredictNothing,
			"-x":             complete.PredictNothing,
			"-asmflags":      complete.PredictAnything,
			"-buildmode":     complete.PredictAnything,
			"-compiler":      complete.PredictAnything,
			"-gccgoflags":    complete.PredictAnything,
			"-gcflags":       complete.PredictAnything,
			"-installsuffix": complete.PredictAnything,
			"-ldflags":       complete.PredictAnything,
			"-linkshared":    complete.PredictNothing,
			"-pkgdir":        anyPackage,
			"-tags":          complete.PredictAnything,
			"-toolexec":      complete.PredictAnything,
		},
		Args: anyGo,
	}

	run := complete.Command{
		Flags: complete.Flags{
			"-exec": complete.PredictAnything,
		},
		Args: goFiles,
	}

	test := complete.Command{
		Flags: complete.Flags{
			"-args": complete.PredictAnything,
			"-c":    complete.PredictNothing,
			"-exec": complete.PredictAnything,

			"-bench":     predictBenchmark,
			"-benchtime": complete.PredictAnything,
			"-count":     complete.PredictAnything,
			"-cover":     complete.PredictNothing,
			"-covermode": complete.PredictSet("set", "count", "atomic"),
			"-coverpkg":  complete.PredictDirs("*"),
			"-cpu":       complete.PredictAnything,
			"-run":       predictTest,
			"-short":     complete.PredictNothing,
			"-timeout":   complete.PredictAnything,

			"-benchmem":             complete.PredictNothing,
			"-blockprofile":         complete.PredictFiles("*.out"),
			"-blockprofilerate":     complete.PredictAnything,
			"-coverprofile":         complete.PredictFiles("*.out"),
			"-cpuprofile":           complete.PredictFiles("*.out"),
			"-memprofile":           complete.PredictFiles("*.out"),
			"-memprofilerate":       complete.PredictAnything,
			"-mutexprofile":         complete.PredictFiles("*.out"),
			"-mutexprofilefraction": complete.PredictAnything,
			"-outputdir":            complete.PredictDirs("*"),
			"-trace":                complete.PredictFiles("*.out"),
		},
		Args: anyGo,
	}

	fmt := complete.Command{
		Flags: complete.Flags{
			"-n": complete.PredictNothing,
			"-x": complete.PredictNothing,
		},
		Args: anyGo,
	}

	get := complete.Command{
		Flags: complete.Flags{
			"-d":        complete.PredictNothing,
			"-f":        complete.PredictNothing,
			"-fix":      complete.PredictNothing,
			"-insecure": complete.PredictNothing,
			"-t":        complete.PredictNothing,
			"-u":        complete.PredictNothing,
		},
		Args: anyGo,
	}

	generate := complete.Command{
		Flags: complete.Flags{
			"-n":   complete.PredictNothing,
			"-x":   complete.PredictNothing,
			"-v":   complete.PredictNothing,
			"-run": complete.PredictAnything,
		},
		Args: anyGo,
	}

	vet := complete.Command{
		Flags: complete.Flags{
			"-n": complete.PredictNothing,
			"-x": complete.PredictNothing,
		},
		Args: anyGo,
	}

	list := complete.Command{
		Flags: complete.Flags{
			"-e":    complete.PredictNothing,
			"-f":    complete.PredictAnything,
			"-json": complete.PredictNothing,
		},
		Args: complete.PredictOr(anyPackage, ellipsis),
	}

	tool := complete.Command{
		Flags: complete.Flags{
			"-n": complete.PredictNothing,
		},
		Args: complete.PredictAnything,
	}

	clean := complete.Command{
		Flags: complete.Flags{
			"-i": complete.PredictNothing,
			"-r": complete.PredictNothing,
			"-n": complete.PredictNothing,
			"-x": complete.PredictNothing,
		},
		Args: complete.PredictOr(anyPackage, ellipsis),
	}

	env := complete.Command{
		Args: complete.PredictAnything,
	}

	bug := complete.Command{}
	version := complete.Command{}

	fix := complete.Command{
		Args: anyGo,
	}

	// commands that also accepts the build flags
	for name, options := range build.Flags {
		test.Flags[name] = options
		run.Flags[name] = options
		list.Flags[name] = options
		vet.Flags[name] = options
		get.Flags[name] = options
	}

	gogo := complete.Command{
		Sub: complete.Commands{
			"build":    build,
			"install":  build, // install and build have the same flags
			"run":      run,
			"test":     test,
			"fmt":      fmt,
			"get":      get,
			"generate": generate,
			"vet":      vet,
			"list":     list,
			"tool":     tool,
			"clean":    clean,
			"env":      env,
			"bug":      bug,
			"fix":      fix,
			"version":  version,
		},
		GlobalFlags: complete.Flags{
			"-h": complete.PredictNothing,
		},
	}

	complete.New("go", gogo).Run()
}
