package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/swaggo/swag"
)

// The spec served at /swagger/doc.json is rendered at runtime from a
// template in docs/docs.go. If rendering fails, swag silently serves the raw
// template, which is not JSON and breaks Swagger UI; docs/swagger.json can
// still look fine. Check what is actually served.
func TestServedOpenAPISpecIsValidJSON(t *testing.T) {
	doc, err := swag.ReadDoc()
	require.NoError(t, err)
	require.True(t, json.Valid([]byte(doc)), "rendered spec is not valid JSON:\n%.300s", doc)

	var spec struct {
		BasePath string         `json:"basePath"`
		Paths    map[string]any `json:"paths"`
	}
	require.NoError(t, json.Unmarshal([]byte(doc), &spec))
	require.Equal(t, "/api/v1", spec.BasePath)
	require.Contains(t, spec.Paths, "/notifications/batch")
	require.Contains(t, spec.Paths, "/templates", "the {{variable}} examples must survive rendering")
}
