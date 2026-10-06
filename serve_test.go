package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// /api/flag decoded any body as JSON and never looked at where the request came
// from, so any web page open in the same browser could flag clips — a
// cross-site text/plain POST needs no preflight.
func TestFlagChangesMustComeFromTheIndexPage(t *testing.T) {
	req := func(contentType, origin, fetchSite string) bool {
		r := httptest.NewRequest("POST", "http://localhost:8080/api/flag", strings.NewReader("{}"))
		if contentType != "" {
			r.Header.Set("Content-Type", contentType)
		}
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if fetchSite != "" {
			r.Header.Set("Sec-Fetch-Site", fetchSite)
		}
		return sameOriginJSON(r)
	}
	if !req("application/json", "http://localhost:8080", "same-origin") {
		t.Error("the index page's own request was refused")
	}
	if !req("application/json; charset=utf-8", "", "") {
		t.Error("a non-browser client sending JSON was refused")
	}
	for _, c := range []struct{ ct, origin, site string }{
		{"text/plain", "https://evil.example", "cross-site"},
		{"text/plain", "", ""},
		{"application/json", "https://evil.example", ""},
		{"application/json", "", "cross-site"},
		{"application/json", "null", ""},
		{"", "http://localhost:8080", ""},
	} {
		if req(c.ct, c.origin, c.site) {
			t.Errorf("accepted %+v", c)
		}
	}
}
