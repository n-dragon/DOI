// Bundled into viewer/vendor/force-graph-widget.js by build.sh (esbuild,
// IIFE, no externals — the whole point is one self-hosted file with zero
// runtime CDN dependency, consistent with the rest of this page). Exposes
// window.DOIForceGraph, a small imperative API so index.html's existing
// vanilla-JS query/status logic (computeStatuses, wireUp, ...) doesn't
// itself need to be React — only this one diagram widget is a React
// component internally.
//
// Small circles, physics-positioned, auto-colored by label — the
// Unused permissions tab renders its full ~1000-node dataset with this,
// not a handful of hand-positioned boxes (that approach doesn't scale
// past a few nodes; see the git history of this file for the version
// that drew them). Status (included/excluded) is a color + size override
// on top of the label color, not a second custom shape.
import React from 'react';
import { createRoot } from 'react-dom/client';
import ForceGraph2D from 'react-force-graph-2d';

const { useRef, useEffect, useState, useMemo, useCallback } = React;

function cssVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

// Read once per mount (this page has no live light/dark toggle — theme is
// decided by prefers-color-scheme/data-theme before first paint).
function readPalette() {
  return {
    inkFaint: cssVar('--ink-faint', '#948da8'),
    excludedStroke: cssVar('--excluded-stroke', '#d3cbe6'),
    danger: cssVar('--danger', '#a3244f'),
    nodeCompute: cssVar('--node-compute', '#3f7fe0'),
    nodeRole: cssVar('--node-role', '#6b4fa0'),
    nodeStore: cssVar('--node-store', '#b2504a'),
  };
}

const LABEL_COLOR = {
  Resource: 'nodeCompute',
  IAMRole: 'nodeRole',
  DataStore: 'nodeStore',
};

const LABEL_VAL = {
  Resource: 1,
  IAMRole: 2.5,
  DataStore: 2.5,
};

function colorFor(node, mode, pal) {
  if (mode === 'query') {
    if (node.status === 'included') return pal.danger;
    return pal.excludedStroke;
  }
  return pal[LABEL_COLOR[node.label]] || pal.nodeCompute;
}

function valFor(node, mode) {
  const base = LABEL_VAL[node.label] || 1;
  if (mode === 'query' && node.status === 'included') return base * 2.2;
  return base;
}

function GraphWidget({ nodes, links, mode, height }) {
  const fgRef = useRef();
  const containerRef = useRef();
  const [width, setWidth] = useState(900);
  const pal = useMemo(readPalette, []);

  useEffect(() => {
    if (!containerRef.current) return;
    const el = containerRef.current;
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect?.width;
      if (w) setWidth(Math.max(320, Math.round(w)));
    });
    ro.observe(el);
    setWidth(Math.max(320, Math.round(el.clientWidth)));
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    const t = setTimeout(() => {
      if (fgRef.current) fgRef.current.zoomToFit(300, 24);
    }, 600);
    return () => clearTimeout(t);
  }, [width, height, nodes, links]);

  const nodeColor = useCallback((node) => colorFor(node, mode, pal), [mode, pal]);
  const nodeVal = useCallback((node) => valFor(node, mode), [mode]);
  const nodeLabel = useCallback((n) => {
    const status =
      n.status === 'included' ? ' · unused permission' : n.status === 'excluded' ? ' · in use / not applicable' : '';
    return `${n.label} · ${n.name}${status}`;
  }, []);

  return React.createElement(
    'div',
    { ref: containerRef, style: { width: '100%' } },
    React.createElement(ForceGraph2D, {
      ref: fgRef,
      graphData: { nodes, links },
      width,
      height,
      backgroundColor: 'rgba(0,0,0,0)',
      nodeColor,
      nodeVal,
      nodeLabel,
      nodeRelSize: 3,
      linkColor: () => pal.inkFaint,
      linkWidth: 0.4,
      linkLabel: (l) => l.title || '',
      cooldownTicks: 120,
      enableNodeDrag: true,
    })
  );
}

const roots = new WeakMap();

window.DOIForceGraph = {
  render(container, props) {
    let root = roots.get(container);
    if (!root) {
      root = createRoot(container);
      roots.set(container, root);
    }
    root.render(React.createElement(GraphWidget, props));
  },
};
