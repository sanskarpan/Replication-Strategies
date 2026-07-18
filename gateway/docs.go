package gateway

import "net/http"

// This file wires a self-hosted API documentation experience served directly by the
// Go gateway from the embedded OpenAPI spec (see openapi_embed.go).
//
// Two routes are intended to be registered by the router wiring (done separately):
//
//	GET /openapi.yaml -> OpenAPIHandler() serves the raw OpenAPI 3 spec as application/yaml
//	GET /docs         -> DocsHandler()    serves a Swagger UI page pointed at /openapi.yaml
//
// The Swagger UI assets are loaded from the jsDelivr CDN (swagger-ui-dist), so no
// static asset bundling is required.

// docsHTML is the Swagger UI page. It loads the swagger-ui-dist bundle from jsDelivr
// and points it at the self-hosted spec URL "/openapi.yaml".
const docsHTML = `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Replication Strategies API — Docs</title>
    <link
      rel="stylesheet"
      href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css"
    />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js" crossorigin></script>
    <script>
      window.onload = function () {
        window.ui = SwaggerUIBundle({
          url: "/openapi.yaml",
          dom_id: "#swagger-ui",
          deepLinking: true,
          presets: [
            SwaggerUIBundle.presets.apis,
            SwaggerUIStandalonePreset,
          ],
          layout: "StandaloneLayout",
        });
      };
    </script>
  </body>
</html>
`

// handleOpenAPISpec serves the embedded OpenAPI spec as application/yaml.
// Intended route: GET /openapi.yaml
func (s *Server) handleOpenAPISpec(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(openAPISpec)
}

// handleDocsUI serves the Swagger UI documentation page.
// Intended route: GET /docs
func (s *Server) handleDocsUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(docsHTML))
}

// OpenAPIHandler returns the handler serving the raw OpenAPI spec, so route wiring can
// register it (intended route: GET /openapi.yaml).
func (s *Server) OpenAPIHandler() http.HandlerFunc {
	return s.handleOpenAPISpec
}

// DocsHandler returns the handler serving the Swagger UI docs page, so route wiring can
// register it (intended route: GET /docs).
func (s *Server) DocsHandler() http.HandlerFunc {
	return s.handleDocsUI
}
