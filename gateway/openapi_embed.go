package gateway

import _ "embed"

// openAPISpec is a local copy of docs/openapi.yaml embedded into the binary so the
// gateway can self-host the API spec (and the Swagger UI docs page) without depending
// on the file being present on disk at runtime.
//
// //go:embed cannot reference a file outside the package directory, so the canonical
// spec at docs/openapi.yaml is mirrored here as gateway/openapi.yaml and embedded.
//
//go:embed openapi.yaml
var openAPISpec []byte
