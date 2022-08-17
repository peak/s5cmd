package command

import (
	"context"
	"fmt"
	"strings"

	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
	"github.com/urfave/cli/v2"
)

func printS3Suggestions(ctx *cli.Context, arg string) {
	// fmt.Println("here!", first)

	c := ctx.Context
	u, err := url.New(arg)
	if err != nil {
		u = &url.URL{Type: 0, Scheme: "s3"}
	}
	client, err := storage.NewRemoteClient(c, u, NewStorageOpts(ctx))
	if err != nil {
		// fmt.Println(escapeColon(err.Error()))
		return
	}

	// fmt.Println("there", first)
	if u.Bucket == "" || (u.IsBucket() && !strings.HasSuffix(arg, "/")) {
		printListBuckets(c, client, u)
	} else {
		printListNURLSuggestions(c, client, u, 20)
	}
}

func printListBuckets(ctx context.Context, client *storage.S3, u *url.URL) {
	buckets, err := client.ListBuckets(ctx, u.Bucket)
	if err != nil {
		fmt.Println(escapeColon(err.Error()))
		return
	}

	for _, bucket := range buckets {
		fmt.Println(escapeColon("s3://" + bucket.Name)) //+" ", u.Absolute()))
	}
}

func printListNURLSuggestions(ctx context.Context, client *storage.S3, u *url.URL, count int) {
	abs := u.Absolute()
	if u.IsBucket() {
		abs = abs + "/"
	}
	u, err := url.New(abs + "*")
	if err != nil {
		fmt.Println(escapeColon(err.Error()))
		return
	}

	i := 0
	for obj := range (*client).List(ctx, u, false) {
		if i > count {
			break
		}
		if obj.Err != nil {
			fmt.Println(escapeColon(obj.Err.Error()))
			return
		}
		fmt.Println(escapeColon(obj.URL.Absolute()))

		i++
	}
}
