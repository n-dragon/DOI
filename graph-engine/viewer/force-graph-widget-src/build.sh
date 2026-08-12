#!/usr/bin/env bash
# Rebuilds ../vendor/force-graph-widget.js (entry.js, 2D — Unused
# permissions tab) and ../vendor/force-graph-3d-widget.js (entry-3d.js,
# 3D — Datadog fleet tab) from source. Run from this directory after
# `npm install`.
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

# ~1.5MB (bundles three.js via 3d-force-graph) — deliberately loaded
# lazily by index.html, only once the Datadog fleet tab is first opened.
npx esbuild entry-3d.js \
  --bundle \
  --minify \
  --format=iife \
  --platform=browser \
  --define:process.env.NODE_ENV='"production"' \
  --outfile=../vendor/force-graph-3d-widget.js

echo "wrote ../vendor/force-graph-3d-widget.js"
