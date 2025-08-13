package storage

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/peak/s5cmd/v2/log"
	"gotest.tools/v3/assert"
)

func TestCreateProxyTransport(t *testing.T) {
	// Initialize log system for tests
	log.Init("error", false)

	tests := []struct {
		name        string
		proxyURL    string
		noVerifySSL bool
		expectError bool
	}{
		{
			name:        "empty proxy URL",
			proxyURL:    "",
			noVerifySSL: false,
			expectError: false,
		},
		{
			name:        "http proxy URL",
			proxyURL:    "http://proxy:8080",
			noVerifySSL: false,
			expectError: false,
		},
		{
			name:        "https proxy URL",
			proxyURL:    "https://proxy:8443",
			noVerifySSL: false,
			expectError: false,
		},
		{
			name:        "socks5 proxy URL",
			proxyURL:    "socks5://proxy:1080",
			noVerifySSL: false,
			expectError: false,
		},
		{
			name:        "invalid proxy URL",
			proxyURL:    "://invalid",
			noVerifySSL: false,
			expectError: true,
		},
		{
			name:        "proxy with no verify SSL",
			proxyURL:    "http://proxy:8080",
			noVerifySSL: true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport, err := createProxyTransport(tt.proxyURL, tt.noVerifySSL)

			if tt.expectError {
				assert.Assert(t, err != nil, "expected error for invalid proxy URL")
				return
			}

			assert.NilError(t, err)
			assert.Assert(t, transport != nil)

			// Test that the transport has the expected configuration
			if tt.noVerifySSL {
				assert.Assert(t, transport.TLSClientConfig != nil)
				assert.Assert(t, transport.TLSClientConfig.InsecureSkipVerify)
			}

			// Test proxy function for HTTP/HTTPS proxies
			if tt.proxyURL != "" && tt.proxyURL != "socks5://proxy:1080" {
				req, _ := http.NewRequest("GET", "http://example.com", nil)
				proxyURL, err := transport.Proxy(req)
				assert.NilError(t, err)
				assert.Assert(t, proxyURL != nil)
			}
		})
	}
}

func TestProxyURLParsing(t *testing.T) {
	tests := []struct {
		name     string
		proxyURL string
		expected *url.URL
	}{
		{
			name:     "http proxy",
			proxyURL: "http://proxy:8080",
			expected: &url.URL{Scheme: "http", Host: "proxy:8080"},
		},
		{
			name:     "https proxy",
			proxyURL: "https://proxy:8443",
			expected: &url.URL{Scheme: "https", Host: "proxy:8443"},
		},
		{
			name:     "socks5 proxy",
			proxyURL: "socks5://proxy:1080",
			expected: &url.URL{Scheme: "socks5", Host: "proxy:1080"},
		},
		{
			name:     "proxy with path",
			proxyURL: "http://proxy:8080/path",
			expected: &url.URL{Scheme: "http", Host: "proxy:8080", Path: "/path"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := url.Parse(tt.proxyURL)
			assert.NilError(t, err)
			assert.Equal(t, tt.expected.Scheme, parsed.Scheme)
			assert.Equal(t, tt.expected.Host, parsed.Host)
			if tt.expected.Path != "" {
				assert.Equal(t, tt.expected.Path, parsed.Path)
			}
		})
	}
}

func TestProxyAuthenticationParsing(t *testing.T) {
	tests := []struct {
		name         string
		proxyURL     string
		expectedUser string
		expectedPass string
		expectedHost string
		expectError  bool
	}{
		{
			name:         "http proxy with auth",
			proxyURL:     "http://user:pass@proxy:8080",
			expectedUser: "user",
			expectedPass: "pass",
			expectedHost: "proxy:8080",
			expectError:  false,
		},
		{
			name:         "https proxy with auth",
			proxyURL:     "https://admin:secret@proxy:8443",
			expectedUser: "admin",
			expectedPass: "secret",
			expectedHost: "proxy:8443",
			expectError:  false,
		},
		{
			name:         "socks5 proxy with auth",
			proxyURL:     "socks5://proxyuser:proxypass@proxy:1080",
			expectedUser: "proxyuser",
			expectedPass: "proxypass",
			expectedHost: "proxy:1080",
			expectError:  false,
		},
		{
			name:         "proxy with special chars in password",
			proxyURL:     "http://user:pass@word@proxy:8080",
			expectedUser: "user",
			expectedPass: "pass@word",
			expectedHost: "proxy:8080",
			expectError:  false,
		},
		{
			name:         "proxy with empty password",
			proxyURL:     "http://user@proxy:8080",
			expectedUser: "user",
			expectedPass: "",
			expectedHost: "proxy:8080",
			expectError:  false,
		},
		{
			name:         "proxy with empty username",
			proxyURL:     "http://:pass@proxy:8080",
			expectedUser: "",
			expectedPass: "pass",
			expectedHost: "proxy:8080",
			expectError:  false,
		},
		{
			name:        "invalid proxy URL with malformed auth",
			proxyURL:    "://user:pass@proxy:8080",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := url.Parse(tt.proxyURL)

			if tt.expectError {
				assert.Assert(t, err != nil, "expected error for malformed proxy URL")
				return
			}

			assert.NilError(t, err)
			assert.Equal(t, tt.expectedUser, parsed.User.Username())

			if tt.expectedPass != "" {
				pass, _ := parsed.User.Password()
				assert.Equal(t, tt.expectedPass, pass)
			}

			assert.Equal(t, tt.expectedHost, parsed.Host)
		})
	}
}

func TestProxyAuthenticationTransport(t *testing.T) {
	tests := []struct {
		name        string
		proxyURL    string
		noVerifySSL bool
		expectError bool
	}{
		{
			name:        "http proxy with auth",
			proxyURL:    "http://user:pass@proxy:8080",
			noVerifySSL: false,
			expectError: false,
		},
		{
			name:        "https proxy with auth",
			proxyURL:    "https://admin:secret@proxy:8443",
			noVerifySSL: false,
			expectError: false,
		},
		{
			name:        "socks5 proxy with auth",
			proxyURL:    "socks5://proxyuser:proxypass@proxy:1080",
			noVerifySSL: false,
			expectError: false,
		},
		{
			name:        "proxy with auth and no verify SSL",
			proxyURL:    "http://user:pass@proxy:8080",
			noVerifySSL: true,
			expectError: false,
		},
		{
			name:        "proxy with special chars in auth",
			proxyURL:    "http://user:pass@word@proxy:8080",
			noVerifySSL: false,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport, err := createProxyTransport(tt.proxyURL, tt.noVerifySSL)

			if tt.expectError {
				assert.Error(t, err, "expected error for proxy transport creation")
				return
			}

			assert.NilError(t, err)
			assert.Assert(t, transport != nil)

			// Test that the transport has the expected configuration
			if tt.noVerifySSL {
				assert.Assert(t, transport.TLSClientConfig != nil)
				assert.Assert(t, transport.TLSClientConfig.InsecureSkipVerify)
			}

			// Test proxy function for HTTP/HTTPS proxies
			if strings.Contains(tt.proxyURL, "http") {
				req, _ := http.NewRequest("GET", "http://example.com", nil)
				proxyURL, err := transport.Proxy(req)
				assert.NilError(t, err)
				assert.Assert(t, proxyURL != nil)

				// Verify that credentials are preserved in the proxy URL
				if strings.Contains(tt.proxyURL, "@") {
					assert.Assert(t, proxyURL.User != nil)
					username := proxyURL.User.Username()
					assert.Assert(t, username != "")
				}
			}
		})
	}
}
