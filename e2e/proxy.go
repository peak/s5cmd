package e2e

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// Hop-by-hop headers. These are removed when sent to the backend.
// http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html
var hopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te", // canonicalized version of "TE"
	"Trailers",
	"Transfer-Encoding",
	"Upgrade",
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func delHopHeaders(header http.Header) {
	for _, h := range hopHeaders {
		header.Del(h)
	}
}

type httpProxy struct {
	successReqs int64
	errorReqs   int64
}

func (p *httpProxy) ServeHTTP(wr http.ResponseWriter, req *http.Request) {
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		msg := "unsupported protocol scheme " + req.URL.Scheme
		http.Error(wr, msg, http.StatusBadRequest)
		return
	}
	// We need to explicitly set New Transport with no Proxy to make
	// sure ServeHttp only receives the requests once. Otherwise, it
	// causes httpProxy to call itself infinitely over and over again.
	client := &http.Client{
		Transport: &http.Transport{Proxy: nil},
	}
	// http: Request.RequestURI can't be set in client requests.
	// http://golang.org/src/pkg/net/http/client.go
	req.RequestURI = ""

	delHopHeaders(req.Header)

	resp, err := client.Do(req)
	if err != nil {
		http.Error(wr, "Server Error", http.StatusInternalServerError)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		atomic.AddInt64(&p.successReqs, 1)
	} else {
		atomic.AddInt64(&p.errorReqs, 1)
	}
	delHopHeaders(resp.Header)

	copyHeader(wr.Header(), resp.Header)
	wr.WriteHeader(resp.StatusCode)
	io.Copy(wr, resp.Body)
}

func (p *httpProxy) isSuccessful(totalReqs int64) bool {
	return totalReqs == atomic.LoadInt64(&p.successReqs) && atomic.LoadInt64(&p.errorReqs) == 0
}

func setupProxy(t *testing.T, p *httpProxy) string {
	proxysrv := httptest.NewServer(p)

	t.Cleanup(func() {
		proxysrv.Close()
	})
	return proxysrv.URL
}
