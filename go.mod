module github.com/peak/s5cmd

go 1.13

require (
	github.com/aws/aws-sdk-go v1.17.4
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/go-cmp v0.4.0
	github.com/google/gops v0.3.2
	github.com/hashicorp/go-multierror v1.0.0
	github.com/iancoleman/strcase v0.0.0-20191112232945-16388991a334
	github.com/johannesboyne/gofakes3 v0.0.0-20191228161223-9aee1c78a252
	github.com/kardianos/osext v0.0.0-20170510131534-ae77be60afb1 // indirect
	github.com/karrick/godirwalk v1.15.3
	github.com/kr/pretty v0.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/posener/complete v0.0.0-20171104095702-dc2bc5a81acc
	github.com/stretchr/testify v1.4.0 // indirect
	github.com/termie/go-shutil v0.0.0-20140729215957-bcacb06fecae
	golang.org/x/text v0.3.2 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gotest.tools/v3 v3.0.0
)

replace github.com/johannesboyne/gofakes3 => github.com/igungor/gofakes3 v0.0.3
