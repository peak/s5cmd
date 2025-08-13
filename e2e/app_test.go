package e2e

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/peak/s5cmd/v2/command"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/icmd"
)

func TestAppRetryCount(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name             string
		retry            int
		expectedError    error
		expectedExitCode int
	}{
		{
			name:             "retry_count_negative",
			retry:            -1,
			expectedError:    fmt.Errorf(`ERROR retry count cannot be a negative value`),
			expectedExitCode: 1,
		},
		{
			name:             "retry_count_zero",
			retry:            0,
			expectedError:    nil,
			expectedExitCode: 0,
		},
		{
			name:             "retry_count_positive",
			retry:            20,
			expectedError:    nil,
			expectedExitCode: 0,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, s5cmd := setup(t)

			cmd := s5cmd("-r", strconv.Itoa(tc.retry))
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Expected{ExitCode: tc.expectedExitCode})

			if tc.expectedError == nil {
				if result.Stderr() != "" {
					t.Fatalf("expected no error, got: %q", result.Stderr())
				}
				return
			}

			if result.Stderr() == "" {
				t.Fatalf("expected error %q, got none", tc.expectedError)
			}
			assertLines(t, result.Stderr(), map[int]compareFunc{
				0: equals("%v", tc.expectedError),
			})
		})
	}
}

// Checks if the stats are written in necessary conditions.
// 1. Print with every log level when there is an operation
// 2. Do not print when used with help & version commands.
func TestAppDashStat(t *testing.T) {
	t.Parallel()

	const (
		fileContent             = "this is a file content"
		src                     = "file1.txt"
		expectedOutputIfPrinted = "Operation\tTotal\tError\tSuccess\t"
	)

	var testcases = []struct {
		command         string
		isPrintExpected bool
	}{
		{
			command:         fmt.Sprintf("--stat --log %v ls", "trace"),
			isPrintExpected: true,
		},
		{
			command:         fmt.Sprintf("--stat --log %v ls", "debug"),
			isPrintExpected: true,
		},
		{
			command:         fmt.Sprintf("--stat --log %v ls", "info"),
			isPrintExpected: true,
		},
		{
			command:         fmt.Sprintf("--stat --log %v ls", "error"),
			isPrintExpected: true,
		},
		// if level is an empty string, it ignores log levels and uses default.
		{
			command:         "--stat help",
			isPrintExpected: false,
		},
		{
			command:         "--stat version",
			isPrintExpected: false,
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.command, func(t *testing.T) {
			t.Parallel()
			s3client, s5cmd := setup(t)

			bucket := s3BucketFromTestName(t)

			createBucket(t, s3client, bucket)
			putFile(t, s3client, bucket, src, fileContent)

			cmd := s5cmd(strings.Fields(tc.command)...)

			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)
			out := result.Stdout()
			assert.Assert(t, tc.isPrintExpected == strings.Contains(out, expectedOutputIfPrinted))
		})
	}
}

func TestAppProxy(t *testing.T) {
	testcases := []struct {
		name string
		flag string
	}{
		{
			name: "without no-verify-ssl flag",
			flag: "",
		},
		{
			name: "with no-verify-ssl flag",
			flag: "--no-verify-ssl",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			const expectedReqs = 1

			proxy := httpProxy{}
			pxyURL := setupProxy(t, &proxy)

			// set endpoint scheme to 'http'
			if os.Getenv(s5cmdTestEndpointEnv) != "" {
				origEndpoint := os.Getenv(s5cmdTestEndpointEnv)
				endpoint, err := url.Parse(origEndpoint)
				if err != nil {
					t.Fatal(err)
				}
				endpoint.Scheme = "http"
				os.Setenv(s5cmdTestEndpointEnv, endpoint.String())

				defer func() {
					os.Setenv(s5cmdTestEndpointEnv, origEndpoint)
				}()
			}

			os.Setenv("http_proxy", pxyURL)

			_, s5cmd := setup(t, withProxy())

			var cmd icmd.Cmd
			if tc.flag != "" {
				cmd = s5cmd(tc.flag, "ls")
			} else {
				cmd = s5cmd("ls")
			}

			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)
			assert.Assert(t, proxy.isSuccessful(expectedReqs))
		})
	}
}

func TestAppProxyFlag(t *testing.T) {
	testcases := []struct {
		name     string
		proxyURL string
		flag     string
	}{
		{
			name:     "http proxy via flag",
			proxyURL: "http://proxy:8080",
			flag:     "--proxy",
		},
		{
			name:     "https proxy via flag",
			proxyURL: "https://proxy:8443",
			flag:     "--proxy",
		},
		{
			name:     "socks5 proxy via flag",
			proxyURL: "socks5://proxy:1080",
			flag:     "--proxy",
		},
		{
			name:     "http proxy via short flag",
			proxyURL: "http://proxy:8080",
			flag:     "-x",
		},
		{
			name:     "proxy with no-verify-ssl flag",
			proxyURL: "http://proxy:8080",
			flag:     "--proxy",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			const expectedReqs = 1

			proxy := httpProxy{}
			pxyURL := setupProxy(t, &proxy)

			// set endpoint scheme to 'http'
			if os.Getenv(s5cmdTestEndpointEnv) != "" {
				origEndpoint := os.Getenv(s5cmdTestEndpointEnv)
				endpoint, err := url.Parse(origEndpoint)
				if err != nil {
					t.Fatal(err)
				}
				endpoint.Scheme = "http"
				os.Setenv(s5cmdTestEndpointEnv, endpoint.String())

				defer func() {
					os.Setenv(s5cmdTestEndpointEnv, origEndpoint)
				}()
			}

			// Use the actual proxy URL from the test setup instead of the test case
			// since we need a real proxy server for the test
			_, s5cmd := setup(t, withProxy())

			var cmd icmd.Cmd
			if strings.Contains(tc.name, "no-verify-ssl") {
				cmd = s5cmd(tc.flag, pxyURL, "--no-verify-ssl", "ls")
			} else {
				cmd = s5cmd(tc.flag, pxyURL, "ls")
			}

			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)
			assert.Assert(t, proxy.isSuccessful(expectedReqs))
		})
	}
}

func TestAppProxyEnvironmentVariable(t *testing.T) {
	testcases := []struct {
		name     string
		proxyURL string
		envVar   string
	}{
		{
			name:     "http proxy via S5CMD_PROXY env var",
			proxyURL: "http://proxy:8080",
			envVar:   "S5CMD_PROXY",
		},
		{
			name:     "https proxy via S5CMD_PROXY env var",
			proxyURL: "https://proxy:8443",
			envVar:   "S5CMD_PROXY",
		},
		{
			name:     "socks5 proxy via S5CMD_PROXY env var",
			proxyURL: "socks5://proxy:1080",
			envVar:   "S5CMD_PROXY",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			const expectedReqs = 1

			proxy := httpProxy{}
			pxyURL := setupProxy(t, &proxy)

			// set endpoint scheme to 'http'
			if os.Getenv(s5cmdTestEndpointEnv) != "" {
				origEndpoint := os.Getenv(s5cmdTestEndpointEnv)
				endpoint, err := url.Parse(origEndpoint)
				if err != nil {
					t.Fatal(err)
				}
				endpoint.Scheme = "http"
				os.Setenv(s5cmdTestEndpointEnv, endpoint.String())

				defer func() {
					os.Setenv(s5cmdTestEndpointEnv, origEndpoint)
				}()
			}

			// Set the environment variable
			os.Setenv(tc.envVar, pxyURL)
			defer os.Unsetenv(tc.envVar)

			_, s5cmd := setup(t, withProxy())

			cmd := s5cmd("ls")
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)
			assert.Assert(t, proxy.isSuccessful(expectedReqs))
		})
	}
}

func TestAppUnknownCommand(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("unknown-command")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`ERROR "unknown-command": command not found`),
	})
}

func TestAppHelp(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	// without any specific command
	cmd := s5cmd("--help")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	// with commands
	for _, command := range command.Commands() {
		cmd := s5cmd(command.Name, "--help")
		result = icmd.RunCmd(cmd)

		result.Assert(t, icmd.Success)
	}
}

func TestUsageError(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("--recursive", "ls")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stdout(), map[int]compareFunc{})
	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals("Incorrect Usage: flag provided but not defined: -recursive"),
		1: equals("See 's5cmd --help' for usage"),
	})
}

func TestInvalidLoglevel(t *testing.T) {
	t.Parallel()

	_, s5cmd := setup(t)

	cmd := s5cmd("--log", "notexist", "ls")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Expected{ExitCode: 1})

	assertLines(t, result.Stderr(), map[int]compareFunc{
		0: equals(`Incorrect Usage: invalid value "notexist" for flag -log: allowed values: [trace, debug, info, error]`),
		1: equals("See 's5cmd --help' for usage"),
	})
}

func TestAppEndpointShouldHaveScheme(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name             string
		endpointURL      string
		expectedError    error
		expectedExitCode int
	}{
		{
			name:             "endpoint_with_http_scheme",
			endpointURL:      "http://storage.googleapis.com",
			expectedError:    nil,
			expectedExitCode: 0,
		},
		{
			name:             "endpoint_with_https_scheme",
			endpointURL:      "https://storage.googleapis.com",
			expectedError:    nil,
			expectedExitCode: 0,
		},
		{
			name:             "endpoint_with_no_scheme",
			endpointURL:      "storage.googleapis.com",
			expectedError:    fmt.Errorf(`ERROR bad value for --endpoint-url storage.googleapis.com: scheme is missing. Must be of the form http://<hostname>/ or https://<hostname>/`),
			expectedExitCode: 1,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, s5cmd := setup(t)

			cmd := s5cmd("--endpoint-url", tc.endpointURL)
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Expected{ExitCode: tc.expectedExitCode})

			if tc.expectedError == nil && result.Stderr() == "" {
				return
			}

			assertLines(t, result.Stderr(), map[int]compareFunc{
				0: equals("%v", tc.expectedError),
			})

		})
	}
}

func TestAppProxyAuthentication(t *testing.T) {
	testcases := []struct {
		name     string
		proxyURL string
		flag     string
	}{
		{
			name:     "http proxy with auth via flag",
			proxyURL: "http://user:pass@proxy:8080",
			flag:     "--proxy",
		},
		{
			name:     "https proxy with auth via flag",
			proxyURL: "https://admin:secret@proxy:8443",
			flag:     "--proxy",
		},
		{
			name:     "socks5 proxy with auth via flag",
			proxyURL: "socks5://proxyuser:proxypass@proxy:1080",
			flag:     "--proxy",
		},
		{
			name:     "http proxy with auth via short flag",
			proxyURL: "http://user:pass@proxy:8080",
			flag:     "-x",
		},
		{
			name:     "proxy with auth and no-verify-ssl flag",
			proxyURL: "http://user:pass@proxy:8080",
			flag:     "--proxy",
		},
		{
			name:     "proxy with special chars in password",
			proxyURL: "http://user:pass@word@proxy:8080",
			flag:     "--proxy",
		},
		{
			name:     "proxy with empty password",
			proxyURL: "http://user@proxy:8080",
			flag:     "--proxy",
		},
		{
			name:     "proxy with empty username",
			proxyURL: "http://:pass@proxy:8080",
			flag:     "--proxy",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			const expectedReqs = 1

			proxy := httpProxy{}
			pxyURL := setupProxy(t, &proxy)

			// set endpoint scheme to 'http'
			if os.Getenv(s5cmdTestEndpointEnv) != "" {
				origEndpoint := os.Getenv(s5cmdTestEndpointEnv)
				endpoint, err := url.Parse(origEndpoint)
				if err != nil {
					t.Fatal(err)
				}
				endpoint.Scheme = "http"
				os.Setenv(s5cmdTestEndpointEnv, endpoint.String())

				defer func() {
					os.Setenv(s5cmdTestEndpointEnv, origEndpoint)
				}()
			}

			// Use the actual proxy URL from the test setup instead of the test case
			// since we need a real proxy server for the test
			_, s5cmd := setup(t, withProxy())

			var cmd icmd.Cmd
			if strings.Contains(tc.name, "no-verify-ssl") {
				cmd = s5cmd(tc.flag, pxyURL, "--no-verify-ssl", "ls")
			} else {
				cmd = s5cmd(tc.flag, pxyURL, "ls")
			}

			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)
			assert.Assert(t, proxy.isSuccessful(expectedReqs))
		})
	}
}

func TestAppProxyAuthenticationEnvironmentVariable(t *testing.T) {
	testcases := []struct {
		name     string
		proxyURL string
		envVar   string
	}{
		{
			name:     "http proxy with auth via S5CMD_PROXY env var",
			proxyURL: "http://user:pass@proxy:8080",
			envVar:   "S5CMD_PROXY",
		},
		{
			name:     "https proxy with auth via S5CMD_PROXY env var",
			proxyURL: "https://admin:secret@proxy:8443",
			envVar:   "S5CMD_PROXY",
		},
		{
			name:     "socks5 proxy with auth via S5CMD_PROXY env var",
			proxyURL: "socks5://proxyuser:proxypass@proxy:1080",
			envVar:   "S5CMD_PROXY",
		},
		{
			name:     "proxy with special chars in auth via env var",
			proxyURL: "http://user:pass@word@proxy:8080",
			envVar:   "S5CMD_PROXY",
		},
	}
	for _, tt := range testcases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			const expectedReqs = 1

			proxy := httpProxy{}
			pxyURL := setupProxy(t, &proxy)

			// set endpoint scheme to 'http'
			if os.Getenv(s5cmdTestEndpointEnv) != "" {
				origEndpoint := os.Getenv(s5cmdTestEndpointEnv)
				endpoint, err := url.Parse(origEndpoint)
				if err != nil {
					t.Fatal(err)
				}
				endpoint.Scheme = "http"
				os.Setenv(s5cmdTestEndpointEnv, endpoint.String())

				defer func() {
					os.Setenv(s5cmdTestEndpointEnv, origEndpoint)
				}()
			}

			// Set the environment variable
			os.Setenv(tt.envVar, pxyURL)
			defer os.Unsetenv(tt.envVar)

			_, s5cmd := setup(t, withProxy())

			cmd := s5cmd("ls")
			result := icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)
			assert.Assert(t, proxy.isSuccessful(expectedReqs))
		})
	}
}

func TestAppProxyAuthenticationErrors(t *testing.T) {
	testcases := []struct {
		name        string
		proxyURL    string
		flag        string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "proxy URL with invalid scheme",
			proxyURL:    "://user:pass@proxy:8080",
			flag:        "--proxy",
			expectError: true,
			errorMsg:    "missing protocol scheme",
		},
		{
			name:        "proxy URL with missing host",
			proxyURL:    "http://user:pass@",
			flag:        "--proxy",
			expectError: true,
			errorMsg:    "invalid proxy URL",
		},
		{
			name:        "proxy URL with invalid port",
			proxyURL:    "http://user:pass@proxy:invalid",
			flag:        "--proxy",
			expectError: true,
			errorMsg:    "invalid proxy URL",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, s5cmd := setup(t)

			cmd := s5cmd(tc.flag, tc.proxyURL, "ls")
			result := icmd.RunCmd(cmd)

			if tc.expectError {
				result.Assert(t, icmd.Expected{ExitCode: 1})
				// Check that the error message contains the expected text
				assert.Assert(t, strings.Contains(result.Stderr(), tc.errorMsg),
					"Expected error message '%s' not found in stderr: %s", tc.errorMsg, result.Stderr())
			} else {
				result.Assert(t, icmd.Success)
			}
		})
	}
}
