package e2e

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	jsonpkg "encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/golden"
	"gotest.tools/v3/icmd"
)

func TestSelectCommand(t *testing.T) {
	t.Parallel()

	const (
		region      = "us-east-1"
		accessKeyID = "minioadmin"
		secretKey   = "minioadmin"

		query                   = "SELECT * FROM s3object s"
		queryWithWhereClause    = "SELECT s.id FROM s3object s WHERE s.line='0'"
		queryWithDocumentAccess = "SELECT s.obj0.id FROM s3object s"
	)

	endpoint := os.Getenv(s5cmdTestEndpointEnv)
	if endpoint == "" {
		t.Skipf("skipping the test because %v environment variable is empty", s5cmdTestEndpointEnv)
	}

	type testcase struct {
		name          string
		cmd           []string
		informat      string
		structure     string
		compression   bool
		outformat     string
		expectedValue string
	}

	testcasesByGroup := map[string][]testcase{
		"json": {
			{
				name: "input:json-lines,output:json-lines",
				cmd: []string{
					"select", "json",
					"--query", query,
				},
				informat:  "json",
				structure: "lines",
				outformat: "json",
			},
			{
				name: "input:json-lines,output:csv",
				cmd: []string{
					"select", "json",
					"--output-format", "csv",
					"--query", query,
				},
				informat:  "json",
				structure: "lines",
				outformat: "csv",
			},
			{
				name: "input:json-document,output:json-lines",
				cmd: []string{
					"select", "json",
					"--structure", "document",
					"--query", query,
				},
				informat:  "json",
				structure: "document",
				outformat: "json",
			}, {
				name: "input:json-lines,output:json-lines,compression:gzip",
				cmd: []string{
					"select", "json",
					"--compression", "gzip",
					"--query", query,
				},
				informat:    "json",
				compression: true,
				structure:   "lines",
				outformat:   "json",
			},
			{
				name: "input:json-lines,output:csv,compression:gzip",
				cmd: []string{
					"select", "json",
					"--compression", "gzip",
					"--output-format", "csv",
					"--query", query,
				},
				informat:    "json",
				compression: true,
				structure:   "lines",
				outformat:   "csv",
			},
			{
				name: "input:json-document,output:json-lines,compression:gzip",
				cmd: []string{
					"select", "json",
					"--compression", "gzip",
					"--structure", "document",
					"--query", query,
				},
				informat:    "json",
				compression: true,
				structure:   "document",
				outformat:   "json",
			},
			{
				name: "input:json-lines,output:json-lines,select:with-where",
				cmd: []string{
					"select", "json",
					"--query", queryWithWhereClause,
				},
				informat:      "json",
				structure:     "lines",
				outformat:     "json",
				expectedValue: "{\"id\":\"id0\"}\n",
			},
			{
				name: "input:json-lines,output:json-lines,compression:gzip,select:with-where",
				cmd: []string{
					"select", "json",
					"--compression", "gzip",
					"--query", queryWithWhereClause,
				},
				informat:      "json",
				compression:   true,
				structure:     "lines",
				outformat:     "json",
				expectedValue: "{\"id\":\"id0\"}\n",
			},
			{
				name: "input:json-lines,output:csv,compression:gzip,select:with-where",
				cmd: []string{
					"select", "json",
					"--compression", "gzip",
					"--output-format", "csv",
					"--query", queryWithWhereClause,
				},
				informat:      "json",
				compression:   true,
				structure:     "lines",
				outformat:     "csv",
				expectedValue: "id0\n",
			},
			{
				name: "input:json-lines,output:csv,select:with-where",
				cmd: []string{
					"select", "json",
					"--output-format", "csv",
					"--query", queryWithWhereClause,
				},
				informat:      "json",
				structure:     "lines",
				outformat:     "csv",
				expectedValue: "id0\n",
			},
			{
				name: "input:json-document,output:json-lines,select:with-document-access",
				cmd: []string{
					"select", "json",
					"--structure", "document",
					"--query", queryWithDocumentAccess,
				},
				informat:      "json",
				structure:     "document",
				outformat:     "json",
				expectedValue: "{\"id\":\"id0\"}\n",
			},
			{
				name: "input:json-document,output:json-lines,compression:gzip,select:with-document-access",
				cmd: []string{
					"select", "json",
					"--structure", "document",
					"--compression", "gzip",
					"--query", queryWithDocumentAccess,
				},
				informat:      "json",
				compression:   true,
				structure:     "document",
				outformat:     "json",
				expectedValue: "{\"id\":\"id0\"}\n",
			},
			{
				name: "input:json-document,output:csv,select:with-document-access",
				cmd: []string{
					"select", "json",
					"--structure", "document",
					"--output-format", "csv",
					"--query", queryWithDocumentAccess,
				},
				informat:      "json",
				structure:     "document",
				outformat:     "csv",
				expectedValue: "id0\n",
			},
			{
				name: "input:json-document,output:csv,compression:gzip,select:with-document-access",
				cmd: []string{
					"select", "json",
					"--structure", "document",
					"--compression", "gzip",
					"--output-format", "csv",
					"--query", queryWithDocumentAccess,
				},
				informat:      "json",
				compression:   true,
				structure:     "document",
				outformat:     "csv",
				expectedValue: "id0\n",
			},
			{
				name: "input:json-lines,output:json-lines,all-versions:true,empty-bucket:true",
				cmd: []string{
					"select", "json",
					"--all-versions",
					"--query", query,
				},
				informat:      "json",
				structure:     "lines",
				outformat:     "json",
				expectedValue: "-",
			},
		},
		"csv": {
			{
				name: "input:csv,output:json,delimiter:comma",
				cmd: []string{
					"select", "csv",
					"--output-format", "json",
					"--query", query,
				},
				informat:  "csv",
				structure: ",",
				outformat: "json",
			},
			{
				name: "input:csv,output:csv,delimiter:comma,extra-flag:false",
				cmd: []string{
					"select", "csv",
					"--query", query,
				},
				informat:  "csv",
				structure: ",",
				outformat: "csv",
			},
			{
				name: "input:csv,output:csv,delimiter:comma,compression:gzip,extra-flag:false",
				cmd: []string{
					"select", "csv",
					"--compression", "gzip",
					"--query", query,
				},
				informat:    "csv",
				compression: true,
				structure:   ",",
				outformat:   "csv",
			},
			{
				name: "input:csv,output:json,delimiter:comma,compression:gzip",
				cmd: []string{
					"select", "csv",
					"--compression", "gzip",
					"--output-format", "json",
					"--query", query,
				},
				informat:    "csv",
				compression: true,
				structure:   ",",
				outformat:   "json",
			},
			{
				name: "input:csv,output:csv,delimiter:comma,compression:gzip",
				cmd: []string{
					"select", "csv",
					"--compression", "gzip",
					"--output-format", "csv",
					"--query", query,
				},
				informat:    "csv",
				compression: true,
				structure:   ",",
				outformat:   "csv",
			},
			{
				name: "input:csv,output:csv,delimiter:comma",
				cmd: []string{
					"select", "csv",
					"--output-format", "csv",
					"--query", query,
				},
				informat:  "csv",
				structure: ",",
				outformat: "csv",
			},
			{
				name: "input:csv,output:csv,delimiter:tab,extra-flag:false",
				cmd: []string{
					"select", "csv",
					"--delimiter", "\t",
					"--query", query,
				},
				informat:  "csv",
				structure: "\t",
				outformat: "csv",
			},
			{
				name: "input:csv,output:json,delimiter:tab",
				cmd: []string{
					"select", "csv",
					"--delimiter", "\t",
					"--output-format", "json",
					"--query", query,
				},
				informat:  "csv",
				structure: "\t",
				outformat: "json",
			},
			{
				name: "input:csv,output:csv,delimiter:tab",
				cmd: []string{
					"select", "csv",
					"--delimiter", "\t",
					"--output-format", "csv",
					"--query", query,
				},
				informat:  "csv",
				structure: "\t",
				outformat: "csv",
			},
			{
				name: "input:csv,output:csv,delimiter:comma,select:with-where",
				cmd: []string{
					"select", "csv",
					"--output-format", "csv",
					"--use-header", "USE",
					"--query", queryWithWhereClause,
				},
				informat:      "csv",
				structure:     ",",
				outformat:     "csv",
				expectedValue: "id0\n",
			},
			{
				name: "input:csv,output:csv,delimiter:comma,compression:gzip,select:with-where",
				cmd: []string{
					"select", "csv",
					"--output-format", "csv",
					"--use-header", "USE",
					"--compression", "gzip",
					"--query", queryWithWhereClause,
				},
				informat:      "csv",
				compression:   true,
				structure:     ",",
				outformat:     "csv",
				expectedValue: "id0\n",
			},
			{
				name: "input:csv,output:csv,delimiter:tab,select:with-where",
				cmd: []string{
					"select", "csv",
					"--output-format", "csv",
					"--delimiter", "\t",
					"--use-header", "USE",
					"--query", queryWithWhereClause,
				},
				informat:      "csv",
				structure:     "\t",
				outformat:     "csv",
				expectedValue: "id0\n",
			},
			{
				name: "input:csv,output:csv,delimiter:delimiter,compression:gzip,select:with-where",
				cmd: []string{
					"select", "csv",
					"--output-format", "csv",
					"--delimiter", "\t",
					"--use-header", "USE",
					"--compression", "gzip",
					"--query", queryWithWhereClause,
				},
				informat:      "csv",
				compression:   true,
				structure:     "\t",
				outformat:     "csv",
				expectedValue: "id0\n",
			},
			{
				name: "input:csv,output:json,delimiter:comma,select:with-where",
				cmd: []string{
					"select", "csv",
					"--output-format", "json",
					"--use-header", "USE",
					"--query", queryWithWhereClause,
				},
				informat:      "csv",
				structure:     ",",
				outformat:     "csv",
				expectedValue: "{\"id\":\"id0\"}\n",
			},
			{
				name: "input:csv,output:json,delimiter:comma,compression:gzip,select:with-where",
				cmd: []string{
					"select", "csv",
					"--output-format", "json",
					"--use-header", "USE",
					"--compression", "gzip",
					"--query", queryWithWhereClause,
				},
				informat:      "csv",
				compression:   true,
				structure:     ",",
				outformat:     "json",
				expectedValue: "{\"id\":\"id0\"}\n",
			},
			{
				name: "input:csv,output:json,delimiter:tab,select:with-where",
				cmd: []string{
					"select", "csv",
					"--output-format", "json",
					"--delimiter", "\t",
					"--use-header", "USE",
					"--query", queryWithWhereClause,
				},
				informat:      "csv",
				structure:     "\t",
				outformat:     "json",
				expectedValue: "{\"id\":\"id0\"}\n",
			},
			{
				name: "input:csv,output:json,delimiter:delimiter,compression:gzip,select:with-where",
				cmd: []string{
					"select", "csv",
					"--output-format", "json",
					"--delimiter", "\t",
					"--use-header", "USE",
					"--compression", "gzip",
					"--query", queryWithWhereClause,
				},
				informat:      "csv",
				compression:   true,
				structure:     "\t",
				outformat:     "json",
				expectedValue: "{\"id\":\"id0\"}\n",
			},
			{
				name: "input:csv,output:csv,delimiter:comma,all-versions:true,empty-bucket:true",
				cmd: []string{
					"select", "csv",
					"--all-versions",
					"--query", query,
				},
				informat:      "csv",
				structure:     ",",
				outformat:     "csv",
				expectedValue: "-",
			},
		},
		"backwards-compatibility": {
			{
				name: "input:json-lines,output:json-lines",
				cmd: []string{
					"select",
					"--query", query,
				},
				informat:  "json",
				structure: "lines",
				outformat: "json",
			},
			{
				name: "input:json-lines,output:json-lines,compression:gzip",
				cmd: []string{
					"select",
					"--query", query,
					"--compression", "gzip",
				},
				informat:    "json",
				compression: true,
				structure:   "lines",
				outformat:   "json",
			},
		},
	}
	for testgroup, testcases := range testcasesByGroup {
		testgroup := testgroup
		testcases := testcases
		t.Run(testgroup, func(t *testing.T) {
			for _, tc := range testcases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					bucket := s3BucketFromTestName(t)
					const rowcount = 5
					contents, expected := genTestData(t, rowcount, tc.informat, tc.outformat, tc.structure, false)

					if tc.expectedValue != "" {
						expected = tc.expectedValue
					}

					var src, filename string
					if tc.compression {
						b := bytes.Buffer{}
						gz := gzip.NewWriter(&b)
						filename = fmt.Sprintf("file.%s.gz", tc.informat)
						src = fmt.Sprintf("s3://%s/%s", bucket, filename)

						if _, err := gz.Write([]byte(contents)); err != nil {
							t.Errorf("could not compress the input object. error: %v\n", err)
						}

						if err := gz.Close(); err != nil {
							t.Errorf("could not close the compressor. error: %v\n", err)
						}

						contents = b.String()
					} else {
						filename = fmt.Sprintf("file.%s", tc.informat)
						src = fmt.Sprintf("s3://%s/%s", bucket, filename)
					}

					s3client, s5cmd := setup(t, withEndpointURL(endpoint), withRegion(region), withAccessKeyID(accessKeyID), withSecretKey(secretKey))
					createBucket(t, s3client, bucket)

					if tc.expectedValue == "-" { // test empty bucket case
						src = fmt.Sprintf("s3://%s/", bucket)
						expected = ""
					} else {
						putFile(t, s3client, bucket, filename, contents)
					}
					tc.cmd = append(tc.cmd, src)
					cmd := s5cmd(tc.cmd...)

					result := icmd.RunCmd(cmd, withEnv("AWS_ACCESS_KEY_ID", accessKeyID), withEnv("AWS_SECRET_ACCESS_KEY", secretKey))

					result.Assert(t, icmd.Success)
					assert.DeepEqual(t, expected, result.Stdout())
				})
			}
		})
	}
}

func TestSelectWithParquet(t *testing.T) {
	// NOTE(deniz): We are skipping this test until the image we use in the
	// service container releases parquet support for select api.

	// TODO(deniz): When bitnami releases a version that is stable for parquet
	// queries, enable this test.
	t.Skip()
	t.Parallel()

	const (
		region      = "us-east-1"
		accessKeyID = "minioadmin"
		secretKey   = "minioadmin"

		query = "SELECT * FROM s3object s"
	)

	endpoint := os.Getenv(s5cmdTestEndpointEnv)
	if endpoint == "" {
		t.Skipf("skipping the test because %v environment variable is empty", s5cmdTestEndpointEnv)
	}

	testcases := []struct {
		name     string
		src      string
		cmd      []string
		expected string
	}{
		{
			name: "in:parquet,output:json",
			src:  "five_line_simple.parquet",
			cmd: []string{
				"select", "parquet",
				"--query", query,
			},
			expected: "output.json",
		},
		{
			name: "in:parquet,output,output:csv",
			src:  "five_line_simple.parquet",
			cmd: []string{
				"select", "parquet",
				"--output-format", "csv",
				"--query", query,
			},
			expected: "output.csv",
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			bucket := s3BucketFromTestName(t)
			src := fmt.Sprintf("s3://%s/%s", bucket, tc.src)
			tc.cmd = append(tc.cmd, src)

			s3client, s5cmd := setup(t, withEndpointURL(endpoint), withRegion(region), withAccessKeyID(accessKeyID), withSecretKey(secretKey))
			createBucket(t, s3client, bucket)

			input := golden.Get(t, filepath.Join("parquet", tc.src))
			putFile(t, s3client, bucket, tc.src, string(input))

			cmd := s5cmd(tc.cmd...)
			result := icmd.RunCmd(cmd, withEnv("AWS_ACCESS_KEY_ID", accessKeyID), withEnv("AWS_SECRET_ACCESS_KEY", secretKey))

			result.Assert(t, icmd.Success)
			assert.Assert(t, golden.String(result.Stdout(), filepath.Join("parquet", tc.expected)))
		})
	}
}

func genTestData(t *testing.T, rowcount int, informat, outformat, structure string, where bool) (string, string) {
	t.Helper()

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

	for i := 0; i < rowcount; i++ {
		data = append(data, row{
			Line: fmt.Sprintf("%d", i),
			ID:   fmt.Sprintf("id%d", i),
			Data: fmt.Sprintf("some event %d", i),
		})
	}

	switch informat {
	case "json":
		encoder := jsonpkg.NewEncoder(&input)

		switch structure {
		case "document":
			rows := make(map[string]row)

			for i, v := range data {
				rows[fmt.Sprintf("obj%d", i)] = v
			}

			if err := encoder.Encode(rows); err != nil {
				t.Fatal(err)
			}

			return input.String(), input.String()
		default:
			for _, d := range data {
				err := encoder.Encode(d)

				if err != nil {
					t.Fatal(err)
				}
			}

			switch outformat {
			case "json":
				return input.String(), input.String()
			case "csv":
				writer := csv.NewWriter(&expected)
				for _, d := range data {
					if err := writer.Write([]string{d.Line, d.ID, d.Data}); err != nil {
						t.Fatal(err)
					}
				}

				writer.Flush()

				return input.String(), expected.String()
			}
		}
	case "csv":
		writer := csv.NewWriter(&input)
		// set the delimiter for the input
		writer.Comma = []rune(structure)[0]
		writer.Write([]string{"line", "id", "data"})

		for _, d := range data {
			writer.Write([]string{d.Line, d.ID, d.Data})
		}

		writer.Flush()

		switch outformat {
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
			writer.Comma = []rune(structure)[0]
			writer.Write([]string{"line", "id", "data"})

			for _, d := range data {
				writer.Write([]string{d.Line, d.ID, d.Data})
			}

			writer.Flush()

			return input.String(), expected.String()
		}
	}
	t.Fatal("unreachable")
	return "", ""
}
