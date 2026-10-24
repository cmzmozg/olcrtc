// Package web holds the panel's page, embedded in the binary so the UI never
// drifts from the API it calls.
//
// ai-generated: the whole package (panel).
package web

import "embed"

// Assets is the panel's page, scripts and stylesheet.
//
// ai-generated: embed the monitoring and journal script alongside the existing assets.
//
//go:embed index.html app.js monitor.js style.css
var Assets embed.FS
