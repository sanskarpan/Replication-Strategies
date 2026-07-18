package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newDocsTestServer builds a Server sufficient for exercising the docs handlers, which
// depend only on the embedded spec and static HTML (not the orchestrator/bus).
func newDocsTestServer() *Server {
	return NewServer(nil, nil, nil)
}

func TestDocsUIHandler(t *testing.T) {
	s := newDocsTestServer()

	req := httptest.NewRequest(http.MethodGet, "/docs", nil)
	rec := httptest.NewRecorder()

	s.DocsHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("docs UI: expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(strings.ToLower(body), "swagger") {
		t.Fatalf("docs UI body does not contain %q; got: %s", "swagger", body)
	}
}

func TestOpenAPISpecHandler(t *testing.T) {
	s := newDocsTestServer()

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	rec := httptest.NewRecorder()

	s.OpenAPIHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("openapi spec: expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "openapi:") {
		t.Fatalf("openapi spec body does not contain %q", "openapi:")
	}
}
