// go:build external
package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gotest.tools/v3/icmd"
)

/*
query csv to json {_1, _2, ..}, n objects with header
query json(lines) to json {} n objects
query csv to csv a,b,c, with header
query json(lines) to csv  n lines, without header
query json(document) to json -> same object
query json(document) to csv -> not supported(?)
*/
func getSequentialFile(n int, inputForm, outputForm, structure string) (string, map[int]compareFunc) {
	sb := strings.Builder{}
	expectedLines := make(map[int]compareFunc)

	for i := 0; i < n+1; i++ {
		var line string
		switch inputForm {
		case "json":
			switch structure {
			case "lines":
				if i == n {
					break
				}
				line = fmt.Sprintf(`{ "line": "%d", "id": "i%d", data: "some event %d" }`, i, i, i)
			case "document":
				if i == n {
					break
				} else if i == 0 {
					line = fmt.Sprintf(`{\n"obj%d": {"line": "%d", "id": "i%d", data: "some event %d"}`, i, i, i, i)
				} else if i == n-1 {
					line = fmt.Sprintf(`"obj%d": {"line": "%d", "id": "i%d", data: "some event %d"}\n}`, i, i, i, i)
				} else {
					line = fmt.Sprintf(`"obj%d": {"line": "%d", "id": "i%d", data: "some event %d"},`, i, i, i, i)
				}

			}
		case "csv":
			if i == 0 {
				line = fmt.Sprintf("line%sid%sdata", structure, structure)
			} else {
				line = fmt.Sprintf(`%d%si%d%s"some event %d"`, i-1, structure, i-1, structure, i-1)
			}
		}
		sb.WriteString(line)
		sb.WriteString("\n")
		switch outputForm {
		case "json":
			if i == n {
				break
			}
			line = fmt.Sprintf(`{ "line": "%d", "id": "i%d", data: "some event %d" }`, i, i, i)
			expectedLines[i] = equals(line)
		case "csv":
			structure := ","
			if i == 0 {
				line = fmt.Sprintf("line%sid%sdata", structure, structure)
			} else {
				line = fmt.Sprintf(`%d%si%d%s"some event %d"`, i-1, structure, i-1, structure, i-1)
			}
			expectedLines[i] = equals(line)
		default:
			expectedLines[i] = equals(line)
		}
	}

	return sb.String(), expectedLines
}

func startMinio(t *testing.T, ctx context.Context) (testcontainers.Container, string, error) {
	t.Helper()
	port, err := nat.NewPort("", "9000")
	if err != nil {
		t.Errorf("error while building port\n")
		return nil, "", err
	}

	req := testcontainers.ContainerRequest{
		Image:        "minio/minio",
		ExposedPorts: []string{string(port)},
		Cmd:          []string{"server", "/data"},
		WaitingFor:   wait.ForListeningPort(port),
	}

	minioC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}
	host, err := minioC.Host(ctx)
	if err != nil {
		return nil, "", err
	}

	mappedPort, err := minioC.MappedPort(ctx, port)
	if err != nil {
		return nil, "", err
	}
	address := fmt.Sprintf("http://%s:%d", host, mappedPort.Int())
	return minioC, address, nil
}

/*
test cases:
json:

	default input structure and output
	default input structure and output as csv
	document input structure and default output
	document input structure and output as csv

csv:

	default input structure and output
	default input structure and output as csv
	\t input structure and default output
	\t input structure and output as csv

parquet:

	// TODO: discuss about testing
	default input structure and output
	default input structure and output as csv
*/
func TestSelectCommand(t *testing.T) {
	t.Skip()
	t.Parallel()
	/*
		ctx := context.Background()
		container, address, err := startMinio(t, ctx)
		if err != nil {
			t.Fatalf("could not start the container. Error:%v\n", err)
		}

		t.Cleanup(func() {
			if err := container.Terminate(ctx); err != nil {
				t.Errorf("could not terminate the container. Error:%v\n", err)
			}
		})
	*/
	// credentials are same for all test cases
	region := "us-east-1"
	accessKeyId := "minioadmin"
	secretKey := "minioadmin"

	address := "http://127.0.0.1:9000"
	// The query is default for all cases, we want to assert the output
	// is as expected after a query.
	query := "SELECT * FROM s3object s LIMIT 1"
	testcases := []struct {
		name      string
		cmd       []string
		in        string
		structure string
		out       string
		expected  map[int]compareFunc
		src       string
	}{
		// json
		{
			name: "json-lines select with default input structure and output",
			cmd: []string{
				"select",
				"json",
				"--query",
				query,
			},
			in:        "json",
			structure: "lines",
			out:       "json",
		},
		{
			name: "json-lines select with default input structure and csv output",
			cmd: []string{
				"select",
				"json",
				"--output-format",
				"csv",
				"--query",
				query,
			},
			in:        "json",
			structure: "lines",
			out:       "csv",
		},
		{
			name: "json-lines select with document input structure and output",
			cmd: []string{
				"select",
				"json",
				"--structure",
				"document",
				"--query",
				query,
			},
			in:        "json",
			structure: "document",
			out:       "json",
		},
		{
			name: "json-lines select with document input structure and csv output",
			cmd: []string{
				"select",
				"json",
				"--structure",
				"document",
				"--output-format",
				"csv",
				"--query",
				query,
			},
			in:        "json",
			structure: "document",
			out:       "csv",
		},
		// csv
		{
			name: "csv select with default delimiter and output",
			cmd: []string{
				"select",
				"csv",
				"--query",
				query,
			},
			in:        "csv",
			structure: ",",
			out:       "json",
		},
		{
			name: "csv select with default delimiter and csv output",
			cmd: []string{
				"select",
				"csv",
				"--output-format",
				"csv",
				"--query",
				query,
			},
			in:        "csv",
			structure: ",",
			out:       "csv",
		},
		{
			name: "csv select with custom delimiter and default output",
			cmd: []string{
				"select",
				"csv",
				"--delimiter",
				"\t",
				"document",
				"--query",
				query,
			},
			in:        "csv",
			structure: "\t",
			out:       "json",
		},
		{
			name: "csv select with custom delimiter and csv output",
			cmd: []string{
				"select",
				"csv",
				"--delimiter",
				"\t",
				"--output-format",
				"csv",
				"--query",
				query,
			},
			in:        "csv",
			structure: "\t",
			out:       "csv",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			contents, expectedLines := getSequentialFile(5, tc.in, tc.out, tc.structure)
			filename := fmt.Sprintf("file.%s", tc.in)
			bucket := s3BucketFromTestName(t)
			src := fmt.Sprintf("s3://%s/%s", bucket, filename)
			tc.cmd = append(tc.cmd, src)

			s3client, s5cmd := setup(t, withEndpointURL(address), withRegion(region), withAccessKeyId(accessKeyId), withSecretKey(secretKey))

			createBucket(t, s3client, bucket)
			putFile(t, s3client, bucket, filename, contents)

			cmd := s5cmd(tc.cmd...)
			result := icmd.RunCmd(cmd, withEnv("AWS_ACCESS_KEY_ID", accessKeyId), withEnv("AWS_SECRET_ACCESS_KEY", secretKey))
			fmt.Println(result.Stderr())
			assertLines(t, result.Stdout(), expectedLines)
		})
	}
}
