#!/usr/bin/env bash
# Rebuilds ../vendor/force-graph-widget.js from entry.js. Run from this
# directory after `npm install`.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

npx esbuild entry.js \
  --bundle \
  --minify \
  --format=iife \
  --platform=browser \
  --define:process.env.NODE_ENV='"production"' \
  --outfile=../vendor/force-graph-widget.js

echo "wrote ../vendor/force-graph-widget.js"
