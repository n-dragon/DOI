// Bundled into viewer/vendor/force-graph-widget.js by build.sh (esbuild,
// IIFE, no externals — the whole point is one self-hosted file with zero
// runtime CDN dependency, consistent with the rest of this page). Exposes
// window.DOIForceGraph, a small imperative API so index.html's existing
// vanilla-JS query/status logic (computeStatuses, wireUp, ...) doesn't
// itself need to be React — only this one diagram widget is a React
// component internally.
//
// index.html caps what it hands this component to ~20 nodes
// (sampleForDisplay in index.html) — few enough that a small custom
// canvas draw (circle + label, not just react-force-graph-2d's default
// dot) stays legible and cheap, unlike the ~1000-node full fleet this
// used to render (see git history: nodeColor/nodeVal only, no per-node
// text). Color is always the node's label (type) — status
// (included/excluded) only changes the ring and size, never the fill,
// so type stays identifiable in both modes.
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
    ink: cssVar('--ink', '#18132b'),
    inkFaint: cssVar('--ink-faint', '#948da8'),
    excludedFill: cssVar('--excluded-fill', '#f1eef8'),
    excludedStroke: cssVar('--excluded-stroke', '#d3cbe6'),
    excludedText: cssVar('--excluded-text', '#a79cc2'),
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

const LABEL_RADIUS = {
  Resource: 6,
  IAMRole: 8,
  DataStore: 8,
};

function baseColorFor(node, pal) {
  return pal[LABEL_COLOR[node.label]] || pal.nodeCompute;
}

function radiusFor(node, mode) {
  const r = LABEL_RADIUS[node.label] || 6;
  return mode === 'query' && node.status === 'included' ? r * 1.6 : r;
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
      if (fgRef.current) fgRef.current.zoomToFit(300, 40);
    }, 600);
    return () => clearTimeout(t);
  }, [width, height, nodes, links, mode]);

  const nodeCanvasObject = useCallback(
    (node, ctx, globalScale) => {
      const r = radiusFor(node, mode);
      const excluded = mode === 'query' && node.status === 'excluded';
      const included = mode === 'query' && node.status === 'included';

      ctx.beginPath();
      ctx.arc(node.x, node.y, r, 0, 2 * Math.PI);
      ctx.fillStyle = excluded ? pal.excludedFill : baseColorFor(node, pal);
      ctx.fill();
      ctx.lineWidth = included ? 2.5 : 1.4;
      ctx.strokeStyle = included ? pal.danger : excluded ? pal.excludedStroke : baseColorFor(node, pal);
      ctx.stroke();

      // Constant on-screen size regardless of zoom (the usual
      // font-size/globalScale trick) — legible whether zoomToFit landed
      // tight or wide on this run's 20-node sample.
      const fontSize = 11 / globalScale;
      ctx.font = fontSize + 'px ui-sans-serif, -apple-system, "Segoe UI", Roboto, sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      ctx.fillStyle = excluded ? pal.excludedText : pal.ink;
      ctx.fillText(node.name, node.x, node.y + r + 2);
    },
    [mode, pal]
  );

  const nodePointerAreaPaint = useCallback(
    (node, color, ctx) => {
      const r = radiusFor(node, mode);
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.arc(node.x, node.y, r, 0, 2 * Math.PI);
      ctx.fill();
    },
    [mode]
  );

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
      nodeCanvasObject,
      nodePointerAreaPaint,
      nodeLabel,
      linkColor: () => pal.inkFaint,
      linkWidth: 1,
      linkLabel: (l) => l.title || '',
      cooldownTicks: 200,
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
