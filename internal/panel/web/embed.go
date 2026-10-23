// Package web holds the panel's page, embedded in the binary so the UI never
// drifts from the API it calls.
//
// ai-generated: the whole package (panel).
package web

import "embed"

// Assets is the panel's page: index.html, app.js and style.css.
//
//go:embed index.html app.js style.css
var Assets embed.FS
