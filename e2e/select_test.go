package e2e

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	jsonpkg "encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"gotest.tools/v3/icmd"
)

// getFile is a helper for creating file contents and expected values
func getFile(n int, inputForm, outputForm, structure string) (string, string) {
	type row struct {
		Line string `json:"line"`
		ID   string `json:"id"`
		Data string `json:"data"`
	}

	var (
		data     []row
		input    bytes.Buffer
		expected bytes.Buffer
	)

	for i := 0; i < n; i++ {
		data = append(data, row{
			Line: fmt.Sprintf("%d", i),
			ID:   fmt.Sprintf("id%d", i),
			Data: fmt.Sprintf("some event %d", i),
		})
	}

	switch inputForm {
	case "json":
		encoder := jsonpkg.NewEncoder(&input)

		switch structure {
		case "document":
			rows := make(map[string]row)

			for i, v := range data {
				rows[fmt.Sprintf("obj%d", i)] = v
			}

			if err := encoder.Encode(rows); err != nil {
				panic(err)
			}
			return input.String(), input.String()
		default:
			for _, d := range data {
				err := encoder.Encode(d)

				if err != nil {
					panic(err)
				}
			}

			switch outputForm {
			case "json":
				return input.String(), input.String()
			case "csv":
				writer := csv.NewWriter(&expected)

				for _, d := range data {
					if err := writer.Write([]string{d.Line, d.ID, d.Data}); err != nil {
						panic(err)
					}
				}

				writer.Flush()

				return input.String(), expected.String()
			}
		}

		// edge case

	case "csv":
		writer := csv.NewWriter(&input)
		// set the delimiter for the input
		writer.Comma = []rune(structure)[0]
		writer.Write([]string{"line", "id", "data"})

		for _, d := range data {
			writer.Write([]string{d.Line, d.ID, d.Data})
		}

		writer.Flush()

		switch outputForm {
		case "json":
			encoder := jsonpkg.NewEncoder(&expected)
			encoder.Encode(map[string]string{
				"_1": "line",
				"_2": "id",
				"_3": "data",
			})

			for _, d := range data {
				encoder.Encode(map[string]string{
					"_1": d.Line,
					"_2": d.ID,
					"_3": d.Data,
				})
			}
			return input.String(), expected.String()
		case "csv":
			writer := csv.NewWriter(&expected)
			writer.Write([]string{"line", "id", "data"})

			for _, d := range data {
				writer.Write([]string{d.Line, d.ID, d.Data})
			}

			writer.Flush()

			return input.String(), expected.String()
		}
	}
	panic("unreachable")
}

func TestSelectCommandWithGeneratedFiles(t *testing.T) {
	t.Parallel()
	// credentials are same for all test cases
	region := "us-east-1"
	accessKeyID := "minioadmin"
	secretKey := "minioadmin"
	// The query is default for all cases, we want to assert the output
	// is as expected after a query.
	query := "SELECT * FROM s3object s LIMIT 6"

	endpoint := os.Getenv("S3_ENDPOINT")

	if endpoint == "" {
		t.Skipf("skipping the test because S3_ENDPOINT environment variable is empty")
	}

	testcases := []struct {
		name        string
		cmd         []string
		in          string
		structure   string
		compression bool
		out         string
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
		}, {
			name: "json-lines select with gzip compression default input structure and output",
			cmd: []string{
				"select",
				"json",
				"--compression",
				"gzip",
				"--query",
				query,
			},
			in:          "json",
			compression: true,
			structure:   "lines",
			out:         "json",
		},
		{
			name: "json-lines select with gzip compression default input structure and csv output",
			cmd: []string{
				"select",
				"json",
				"--compression",
				"gzip",
				"--output-format",
				"csv",
				"--query",
				query,
			},
			in:          "json",
			compression: true,
			structure:   "lines",
			out:         "csv",
		},
		{
			name: "json-lines select with gzip compression document input structure and output",
			cmd: []string{
				"select",
				"json",
				"--compression",
				"gzip",
				"--structure",
				"document",
				"--query",
				query,
			},
			in:          "json",
			compression: true,
			structure:   "document",
			out:         "json",
		},
		/* {
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
		}, */ // This case is not supported by AWS itself.
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
			name: "csv select with gzip compression default delimiter and output",
			cmd: []string{
				"select",
				"csv",
				"--compression",
				"gzip",
				"--query",
				query,
			},
			in:          "csv",
			compression: true,
			structure:   ",",
			out:         "json",
		},
		{
			name: "csv select with gzip compression default delimiter and csv output",
			cmd: []string{
				"select",
				"csv",
				"--compression",
				"gzip",
				"--output-format",
				"csv",
				"--query",
				query,
			},
			in:          "csv",
			compression: true,
			structure:   ",",
			out:         "csv",
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
		{
			name: "query json with default fallback",
			cmd: []string{
				"select",
				"--query",
				query,
			},
			in:        "json",
			structure: "lines",
			out:       "json",
		},
		{
			name: "query compressed json with default fallback",
			cmd: []string{
				"select",
				"--query",
				query,
				"--compression",
				"gzip",
			},
			in:          "json",
			compression: true,
			structure:   "lines",
			out:         "json",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var src, filename string

			bucket := s3BucketFromTestName(t)
			contents, expected := getFile(5, tc.in, tc.out, tc.structure)

			if tc.compression {
				b := bytes.Buffer{}
				gz := gzip.NewWriter(&b)
				filename = fmt.Sprintf("file.%s.gz", tc.in)
				src = fmt.Sprintf("s3://%s/%s", bucket, filename)

				if _, err := gz.Write([]byte(contents)); err != nil {
					t.Errorf("could not compress the input object. error: %v\n", err)
				}

				if err := gz.Close(); err != nil {
					t.Errorf("could not close the compressor error: %v\n", err)
				}

				contents = b.String()
			} else {
				filename = fmt.Sprintf("file.%s", tc.in)
				src = fmt.Sprintf("s3://%s/%s", bucket, filename)
			}

			s3client, s5cmd := setup(t, withEndpointURL(endpoint), withRegion(region), withAccessKeyID(accessKeyID), withSecretKey(secretKey))

			createBucket(t, s3client, bucket)

			tc.cmd = append(tc.cmd, src)

			putFile(t, s3client, bucket, filename, contents)

			cmd := s5cmd(tc.cmd...)

			result := icmd.RunCmd(cmd, withEnv("AWS_ACCESS_KEY_ID", accessKeyID), withEnv("AWS_SECRET_ACCESS_KEY", secretKey))

			if diff := cmp.Diff(expected, result.Stdout()); diff != "" {
				t.Errorf("select command mismatch (-want +got):\n%s", diff)
			}

		})
	}
}

func TestSelectWithParquet(t *testing.T) {
	t.Parallel()
	// credentials are same for all test cases
	region := "us-east-1"
	accessKeyID := "minioadmin"
	secretKey := "minioadmin"
	// The query is default for all cases, we want to assert the output
	// is as expected after a query.
	query := "SELECT * FROM s3object s LIMIT 6"

	endpoint := os.Getenv("S3_ENDPOINT")

	if endpoint == "" {
		t.Skipf("skipping the test because S3_ENDPOINT environment variable is empty")
	}

	testcases := []struct {
		name     string
		src      string
		cmd      []string
		expected string
	}{
		{
			name: "parquet select with json output",
			src:  "five_line_simple.parquet",
			cmd: []string{
				"select",
				"parquet",
				"--query",
				query,
			},
			expected: "output.json",
		},
		{
			name: "parquet select with json output",
			src:  "five_line_simple.parquet",
			cmd: []string{
				"select",
				"parquet",
				"--output-format",
				"csv",
				"--query",
				query,
			},
			expected: "output.csv",
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			var expectedBuf bytes.Buffer

			// change the working directory to ./e2e/testdata/parquet
			cwd, err := os.Getwd()
			if err != nil {
				t.Fatalf("couldn't reach the current working directory to access testfiles. error: %v\n", err)
			}

			cwd += "/testfiles/parquet/"
			sourceFile, err := os.Open(cwd + tc.src)
			if err != nil {
				t.Fatalf("couldn't read the parquet file to be queried. error: %v\n", err)
			}
			defer sourceFile.Close()

			if _, err := sourceFile.Read(buf.Bytes()); err != nil {
				t.Fatalf("couldn't write the parquet file to buffer. error: %v\n", err)
			}

			expectedFile, err := os.Open(cwd + tc.expected)
			if err != nil {
				t.Fatalf("couldnt read the output file to be compared against. error: %v\n", err)
			}
			defer expectedFile.Close()

			if _, err := expectedFile.Read(expectedBuf.Bytes()); err != nil {
				t.Fatalf("couldn't write the output file to buffer. error: %v\n", err)
			}

			// convert the file content to string
			expected := expectedBuf.String()
			bucket := s3BucketFromTestName(t)
			src := fmt.Sprintf("s3://%s/%s", bucket, tc.src)
			tc.cmd = append(tc.cmd, src)

			s3client, s5cmd := setup(t, withEndpointURL(endpoint), withRegion(region), withAccessKeyID(accessKeyID), withSecretKey(secretKey))
			createBucket(t, s3client, bucket)
			putFile(t, s3client, bucket, tc.src, buf.String())

			cmd := s5cmd(tc.cmd...)
			result := icmd.RunCmd(cmd, withEnv("AWS_ACCESS_KEY_ID", accessKeyID), withEnv("AWS_SECRET_ACCESS_KEY", secretKey))
			if diff := cmp.Diff(expected, result.Stdout()); diff != "" {
				t.Errorf("select command mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
