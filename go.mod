module github.com/peak/s5cmd/v2

go 1.26.0

require (
	github.com/aws/aws-sdk-go-v2 v1.46.0
	github.com/aws/aws-sdk-go-v2/config v1.33.3
	github.com/aws/aws-sdk-go-v2/credentials v1.20.3
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.23.3
	github.com/aws/aws-sdk-go-v2/service/s3 v1.111.0
	github.com/aws/smithy-go v1.28.1
	github.com/cheggaaa/pb/v3 v3.2.1
	github.com/google/go-cmp v0.7.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/iancoleman/strcase v0.3.0
	github.com/igungor/gofakes3 v0.0.18
	github.com/karrick/godirwalk v1.17.0
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51
	github.com/lanrat/extsort v1.4.2
	github.com/termie/go-shutil v0.0.0-20140729215957-bcacb06fecae
	github.com/urfave/cli/v3 v3.11.0
	go.uber.org/mock v0.6.0
	gotest.tools/v3 v3.5.2
)

require (
	github.com/BurntSushi/toml v1.6.0 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/aws/aws-sdk-go v1.55.8 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.19.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.9.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.37.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.42.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.49.0 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mattn/go-runewidth v0.0.29 // indirect
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46 // indirect
	github.com/shabbyrobe/gocovmerge v0.0.0-20230507112040-c3350d9342df // indirect
	go.etcd.io/bbolt v1.5.0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20260824195058-e88cd73687aa // indirect
	golang.org/x/mod v0.40.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/tools v0.49.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22 // indirect
	honnef.co/go/tools v0.8.1 // indirect
	mvdan.cc/unparam v0.0.0-20260823230713-2fa3d841b0c8 // indirect
)

tool (
	go.uber.org/mock/mockgen
	honnef.co/go/tools/cmd/staticcheck
	mvdan.cc/unparam
)
