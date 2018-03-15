package complete

import (
	"context"
	"errors"
	"flag"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/peakgames/s5cmd/core"
	"github.com/peakgames/s5cmd/opt"
	"github.com/peakgames/s5cmd/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	cmp "github.com/posener/complete"
)

const (
	s3CompletionTimeout = 5 * time.Second
	s3MaxKeys           = 20
	s3MaxPages          = 1
)

// ParseFlagsAndRun will initialize shell-completion, and introduce the shell completion specific options. It also calls flag.Parse()
func ParseFlagsAndRun() (bool, error) {
	doInstall := flag.Bool("cmp-install", false, "Install shell completion")
	doUninstall := flag.Bool("cmp-uninstall", false, "Uninstall shell completion")

	completer := cmp.Command{
		Flags: cmp.Flags{
			"-f": cmp.PredictOr(cmp.PredictSet("-"), cmp.PredictFiles("*")),
			"-numworkers": cmp.PredictFunc(func(a cmp.Args) []string {
				// add some sensible defaults...
				ret := []string{"-1", "-2", "-4"}
				nc := float64(runtime.NumCPU())
				if nc > 1 {
					ret = append(ret, strconv.Itoa(int(math.Floor(nc/2))))
				}
				if nc > 4 {
					ret = append(ret, strconv.Itoa(int(math.Floor(nc/4))))
				}
				return ret
			}),
			"-cs":            cmp.PredictSet("5", "16", "64", "128", "256"),
			"-dlp":           cmp.PredictSet("5", "16", "64", "128", "256"),
			"-dlw":           cmp.PredictSet("5", "8", "16", "32", "64"),
			"-cmp-install":   cmp.PredictSet("assume-yes"),
			"-cmp-uninstall": cmp.PredictSet("assume-yes"),
			"-h":             cmp.PredictNothing,
			"-r":             cmp.PredictSet("0", "1", "2", "10", "100"),
			"-stats":         cmp.PredictNothing,
			"-ulw":           cmp.PredictSet("5", "8", "16", "32", "64"),
			"-version":       cmp.PredictNothing,
			"-gops":          cmp.PredictNothing,
			"-vv":            cmp.PredictNothing,
		},
		Sub: getSubCommands(),
	}

	flag.Parse()

	cc := cmp.New("s5cmd", completer)

	if *doInstall && *doUninstall {
		return false, errors.New("Install and uninstall are mutually exclusive")
	} else if *doInstall || *doUninstall {
		return true, setupCompletion(*doInstall)
	}

	return cc.Run(), nil
}

// getSubCommands returns a command vs. flag list for shell completion. It merges each Keyword and its flags into a single list.
func getSubCommands() cmp.Commands {
	ret := make(cmp.Commands)

	// map of command Keyword vs. flags
	flagList := make(map[string]*map[string]struct{})
	// map of command Keyword vs. arg types
	argList := make(map[string]*map[opt.ParamType]struct{})

	for _, c := range core.Commands {
		if c.Operation.IsInternal() {
			continue
		}

		// Do the flags
		flagsForKeyword, ok := flagList[c.Keyword]
		if !ok {
			tmp := make(map[string]struct{})
			flagsForKeyword = &tmp
			flagList[c.Keyword] = flagsForKeyword
		}

		for _, o := range *(c.Operation.GetAcceptedOpts()) {
			optName := o.GetParam()
			if optName == "" {
				continue
			}
			(*flagsForKeyword)[optName] = struct{}{}
		}

		// Now the args
		argsForKeyword, ok := argList[c.Keyword]
		if !ok {
			tmp := make(map[opt.ParamType]struct{})
			argsForKeyword = &tmp
			argList[c.Keyword] = argsForKeyword
		}

		for _, p := range c.Params {
			(*argsForKeyword)[p] = struct{}{}
		}

		if _, ok = ret[c.Keyword]; !ok {
			ret[c.Keyword] = cmp.Command{}
		}
	}

	// Set the flags
	for kw, v := range flagList {
		flgs := make(cmp.Flags)
		for flagVal := range *v {
			flgs[flagVal] = cmp.PredictNothing // our subcommand flags are always boolean
		}
		c := ret[kw]
		c.Flags = flgs
		ret[kw] = c
	}

	// Now the args
	for kw, v := range argList {
		predictorList := make([]cmp.Predictor, 0)
		addedS3predictor := false
		for ptype := range *v {
			switch ptype {
			case opt.FileObj:
				predictorList = append(predictorList, cmp.PredictFiles("*"))
			case opt.Dir, opt.OptionalDir:
				predictorList = append(predictorList, cmp.PredictDirs("*"))
			case opt.FileOrDir, opt.OptionalFileOrDir:
				predictorList = append(predictorList, cmp.PredictFiles("*"), cmp.PredictDirs("*"))

			case opt.Glob:
				fallthrough
			case opt.Unchecked:
				fallthrough
			case opt.UncheckedOneOrMore:
				predictorList = append(predictorList, cmp.PredictAnything)

			case opt.S3Dir:
				fallthrough
			case opt.S3Obj:
				fallthrough
			case opt.S3ObjOrDir:
				fallthrough
			case opt.S3SimpleObj:
				fallthrough
			case opt.S3WildObj:
				if !addedS3predictor {
					predictorList = append(predictorList, cmp.PredictFunc(s3predictor))
					addedS3predictor = true
				}
			}
		}

		c := ret[kw]
		c.Args = cmp.PredictOr(predictorList...)
		ret[kw] = c
	}

	return ret
}

func s3predictor(a cmp.Args) []string {
	if a.Last == "" || a.Last == "s3" || a.Last == "s3:" || a.Last == "s3:/" {
		// Return more than one match so that after "s5cmd ls s3<tab>" completes to s3://, double <tab> lists the buckets without the need for a backspace
		return []string{"s3://a-bucket", "s3://my-bucket"}
	}

	if !strings.HasPrefix(a.Last, "s3://") {
		return nil
	}

	var s3bucket, s3key string
	var endsInSlash bool
	if a.Last == "s3://" {
		s3bucket = ""
		s3key = ""
	} else {
		s3u, err := url.ParseS3Url(a.Last)
		if err != nil {
			return nil
		}
		s3bucket = s3u.Bucket
		s3key = s3u.Key
		endsInSlash = a.Last[len(a.Last)-1] == '/'
	}

	// Quickly create a new session with defaults
	ses, err := core.NewAwsSession(-1, "")
	if err != nil {
		return nil
	}
	client := s3.New(ses)
	ctx, canceler := context.WithTimeout(context.Background(), s3CompletionTimeout)
	defer canceler() // avoid a leak and make go vet happy

	// No object key and (no bucket or not ending in slash char): get S3 buckets
	if s3key == "" && (s3bucket == "" || !endsInSlash) {
		o, err := client.ListBucketsWithContext(ctx, &s3.ListBucketsInput{})
		if err != nil {
			return nil
		}

		var ret []string
		for _, b := range o.Buckets {
			if s3bucket == "" {
				// Return a list of all buckets
				ret = append(ret, "s3://"+*b.Name+"/")
			} else {
				// Check starts-with and only return matching buckets
				if strings.HasPrefix(*b.Name, s3bucket) {
					ret = append(ret, "s3://"+*b.Name+"/")
				}
			}
		}

		// if only 1 match, fall through and list objects in the bucket
		if len(ret) != 1 {
			return ret
		}

		s3bucket = strings.TrimRight(ret[0][5:], "/") // "s3://bucket/" to "bucket"
	}

	if s3bucket != "" {
		// Override default region with bucket
		ses, err := core.GetSessionForBucket(client, s3bucket)
		if err == nil {
			client = s3.New(ses)
		}

		var ret []string

		prefix := "s3://" + s3bucket + "/"

		page := 0
		client.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:    &s3bucket,
			Delimiter: aws.String("/"),
			Prefix:    &s3key,
			MaxKeys:   aws.Int64(s3MaxKeys),
		}, func(o *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, p := range o.CommonPrefixes {
				ret = append(ret, prefix+*p.Prefix)
			}

			for _, q := range o.Contents {
				// Ignore the 0-byte "*_$folder$" objects in shell completion, created by s3n
				if *q.Size == 0 && strings.HasSuffix(*q.Key, "_$folder$") {
					continue
				}

				ret = append(ret, prefix+*q.Key)
			}

			page++
			return page < s3MaxPages
		})

		// If no s3key given, add the bare bucket name to our results
		if s3key == "" {
			ret = append(ret, prefix)
		}

		return ret
	}

	return nil
}
