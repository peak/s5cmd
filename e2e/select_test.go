// go:build external
package e2e

import (
	"fmt"
	"strings"
	"testing"

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
				line = fmt.Sprintf(`{ "line": "%d", "id": "i%d", "data": "some event %d" }`, i, i, i)
			case "document":
				if i == 0 {
					line = fmt.Sprintf("{\n%s", line)
				} else if i == n {
					line = fmt.Sprintf("%s\n}", line)
				} else {
					if i == n-1 {
						line = fmt.Sprintf(`	"obj%d": {"line": "%d", "id": "i%d", "data": "some event %d"}`, i, i, i, i)
					} else {
						line = fmt.Sprintf(`	"obj%d": {"line": "%d", "id": "i%d", "data": "some event %d"},`, i, i, i, i)
					}
				}

			}
		case "csv":
			if i == n {
				break
			}
			if i == 0 {
				line = fmt.Sprintf("line%sid%sdata", structure, structure)
			} else {
				line = fmt.Sprintf(`%d%si%d%s"some event %d"`, i-1, structure, i-1, structure, i-1)
			}
		}
		sb.WriteString(line)
		sb.WriteString("\n")

		switch inputForm {
		case "json":
			// edge case
			if structure == "document" && i == n {
				totalLine := ""
				expectedLines := make(map[int]compareFunc, 1)
				for j := 0; j < n+1; j++ {
					if j == 0 {
						line = "{"
					} else if j == n {
						line = "}"
					} else {
						if j == n-1 {
							line = fmt.Sprintf(`"obj%d":{"line":"%d","id":"i%d","data":"some event %d"}`, j, j, j, j)
						} else {
							line = fmt.Sprintf(`"obj%d":{"line":"%d","id":"i%d","data":"some event %d"},`, j, j, j, j)
						}
					}
					totalLine = fmt.Sprintf("%s%s", totalLine, line)
				}
				expectedLines[0] = equals(totalLine)
				return sb.String(), expectedLines

			}
			if i == n {
				break
			}
			switch outputForm {
			case "csv":
				structure := ","
				line = fmt.Sprintf(`%d%si%d%ssome event %d`, i, structure, i, structure, i)
			case "json":
				if i != n {
					line = fmt.Sprintf(`{"line":"%d","id":"i%d","data":"some event %d"}`, i, i, i)
				}

			}
		case "csv":
			if i == n {
				break
			}
			switch outputForm {
			case "csv":
				structure := ","
				if i == 0 {
					line = fmt.Sprintf("line%sid%sdata", structure, structure)
				} else {
					line = fmt.Sprintf(`%d%si%d%ssome event %d`, i-1, structure, i-1, structure, i-1)
				}
			case "json":
				if i == 0 {
					line = `{"_1":"line","_2":"id","_3":"data"}`
				} else {
					form := `{"_1":"%d","_2":"i%d","_3":"some event %d"}`
					line = fmt.Sprintf(form, i-1, i-1, i-1)
				}
			}
		}
		if i != n {
			expectedLines[i] = equals(line)
		}
	}

	return sb.String(), expectedLines
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
	tab input structure and default output
	tab input structure and output as csv

parquet:

	// TODO: discuss about testing
	default input structure and output
	default input structure and output as csv
*/
func TestSelectCommand(t *testing.T) {
	t.Parallel()
	// credentials are same for all test cases
	region := "us-east-1"
	accessKeyId := "minioadmin"
	secretKey := "minioadmin"
	address := "http://127.0.0.1:9000"
	// The query is default for all cases, we want to assert the output
	// is as expected after a query.
	query := "SELECT * FROM s3object s LIMIT 5"
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

			assertLines(t, result.Stdout(), expectedLines)
		})
	}
}
